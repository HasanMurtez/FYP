from flask import Blueprint, jsonify, request
import requests as http_requests
from app import db
from app.models import Team, Player

# Create blueprint
chat_bp = Blueprint('chat', __name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


def get_squad_context():
    """Build a summary of all teams and key player stats for the AI"""
    teams = Team.query.all()
    context_parts = []

    for team in teams:
        players = Player.query.filter_by(team_id=team.id).all()
        if not players:
            continue

        injured = [p for p in players if p.status == 'i']
        doubtful = [p for p in players if p.status == 'd']
        high_risk = [p for p in players if p.injury_risk_level == 'High']

        # Build concise player list
        player_lines = []
        for p in sorted(players, key=lambda x: x.minutes, reverse=True)[:30]:
            player_lines.append(
                f"  - {p.web_name} ({p.position}): {p.minutes}min, {p.goals_scored}G, "
                f"{p.assists}A, ICT:{p.ict_index}, Status:{p.status_description}, "
                f"Risk:{p.injury_risk_level}({p.injury_risk_score}%), "
                f"Availability:{p.availability_score}/10, "
                f"RecentMins(L5):{p.recent_minutes_last_5}, "
                f"Overworked:{'Yes' if p.is_overworked else 'No'}, "
                f"InjuryProne:{'Yes' if p.injury_prone else 'No'}"
                f"{', News: ' + p.news if p.news else ''}"
            )

        context_parts.append(
            f"\n{team.name} ({team.short_name}) - "
            f"Congestion: {team.congestion_level}, "
            f"Upcoming Fixtures: {team.total_upcoming_fixtures}\n"
            f"Injured: {len(injured)}, Doubtful: {len(doubtful)}, High Risk: {len(high_risk)}\n"
            f"Players:\n" + "\n".join(player_lines)
        )

    return "\n".join(context_parts)


@chat_bp.route('/api/chat', methods=['POST'])
def chat():
    """AI chatbot endpoint - answers football management questions using squad data"""
    try:
        data = request.get_json()
        user_message = data.get('message', '').strip()
        conversation_history = data.get('history', [])

        if not user_message:
            return jsonify({
                'success': False,
                'error': 'Message is required'
            }), 400

        # Get squad data context
        squad_context = get_squad_context()

        # Build the system prompt
        system_prompt = f"""You are an AI Football Manager Assistant for a Premier League club management platform. You have access to real, current player data from the Fantasy Premier League API.

Your role is to help managers make decisions about:
- Whether to play or rest specific players based on injury risk, workload, and form
- Which players are high risk and should be monitored
- Squad rotation recommendations based on fixture congestion
- Player comparisons and scouting suggestions
- General squad analysis and insights

IMPORTANT GUIDELINES:
- Give clear, confident recommendations backed by the data
- Reference specific stats when making points (minutes, goals, injury risk %, availability score)
- Keep responses concise but informative - like a real assistant manager briefing
- If a player is High Risk or Injured, always flag it clearly
- Consider fixture congestion when advising on rotation
- Be conversational and football-savvy, not robotic
- If asked about a player not in the data, say you don't have data on them
- Use the injury risk score percentage to indicate confidence of the prediction

CURRENT SQUAD DATA:
{squad_context}"""

        # Build messages for the API
        messages = []
        for msg in conversation_history[-10:]:  # Keep last 10 messages for context
            messages.append({
                'role': msg['role'],
                'content': msg['content']
            })
        messages.append({
            'role': 'user',
            'content': user_message
        })

        # Call Claude API
        response = http_requests.post(
            ANTHROPIC_API_URL,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': data.get('api_key', ''),
                'anthropic-version': '2023-06-01'
            },
            json={
                'model': 'claude-sonnet-4-6-20250415',
                'max_tokens': 800,
                'system': system_prompt,
                'messages': messages
            },
            timeout=30
        )

        if response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'AI API error: {response.status_code}'
            }), 500

        ai_response = response.json()
        reply = ai_response['content'][0]['text']

        return jsonify({
            'success': True,
            'data': {
                'reply': reply
            }
        }), 200

    except Exception as e:
        print(f"Chat error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500