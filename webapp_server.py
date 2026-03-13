import json
import hashlib
import hmac
import os
import threading
from datetime import datetime
from urllib.parse import parse_qsl

from flask import Flask, request, jsonify, send_file


def _parse_webapp_user(init_data: str, bot_token: str):
    """Валидирует Telegram WebApp initData и возвращает профиль пользователя."""
    if not init_data:
        raise ValueError("initData is required")

    data = dict(parse_qsl(init_data, keep_blank_values=True))
    hash_value = data.pop('hash', None)
    if not hash_value:
        raise ValueError('Invalid initData: hash is missing')

    check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calculated_hash, hash_value):
        raise ValueError('Invalid initData signature')

    user = json.loads(data.get('user', '{}'))
    if not user or 'id' not in user:
        raise ValueError('User payload not found')

    return {
        'telegram_id': int(user['id']),
        'username': user.get('username', ''),
        'first_name': user.get('first_name', 'Пользователь'),
        'avatar': user.get('photo_url', '')
    }


def _normalize_courier_input(raw_text: str):
    """Парсит строку вида 'Фамилия Имя Город' или 'Фамилия Имя, Город'."""
    raw = (raw_text or '').strip()
    if ',' in raw:
        left, right = [part.strip() for part in raw.split(',', 1)]
        fio_parts = left.split()
        city = right
    else:
        parts = raw.split()
        fio_parts = parts[:2]
        city = " ".join(parts[2:])

    if len(fio_parts) < 2 or not city:
        raise ValueError('Используйте формат: Фамилия Имя Город')

    fio = " ".join(fio_parts[:2])
    return fio, city


def _upsert_webapp_user(get_db, user_data):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''INSERT INTO webapp_users(telegram_id, username, first_name, avatar, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET
           username=excluded.username,
           first_name=excluded.first_name,
           avatar=excluded.avatar,
           updated_at=excluded.updated_at''',
        (
            user_data['telegram_id'],
            user_data['username'],
            user_data['first_name'],
            user_data['avatar'],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()


def _require_webapp_user(get_db, init_data: str, bot_token: str):
    user = _parse_webapp_user(init_data, bot_token)
    _upsert_webapp_user(get_db, user)
    return user


def _get_webapp_stats(get_db, owner_id):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT COUNT(*),
                  SUM(CASE WHEN status='accepted' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),
                  SUM(reward)
           FROM webapp_leads
           WHERE owner_telegram_id = ?''',
        (owner_id,),
    )
    total, accepted, pending, reward = c.fetchone()
    return {
        'total': total or 0,
        'accepted': accepted or 0,
        'pending': pending or 0,
        'reward': float(reward or 0),
    }


def _fetch_leads_for_user(get_db, user_id: int, include_all: bool = False):
    conn = get_db()
    c = conn.cursor()
    base_query = '''SELECT l.id, l.owner_telegram_id, l.fio, l.city, l.status, l.orders, l.reward, l.created_at, u.avatar
                    FROM webapp_leads l
                    LEFT JOIN webapp_users u ON l.owner_telegram_id = u.telegram_id'''
    if include_all:
        c.execute(base_query + ' ORDER BY l.id DESC')
    else:
        c.execute(base_query + ' WHERE l.owner_telegram_id = ? ORDER BY l.id DESC', (user_id,))

    return [
        {
            'id': row[0],
            'owner_telegram_id': row[1],
            'fio': row[2],
            'city': row[3],
            'status': row[4],
            'orders': row[5],
            'reward': row[6],
            'created_at': row[7],
            'owner_avatar': row[8],
        }
        for row in c.fetchall()
    ]


def build_webapp(get_db, is_admin, token):
    app = Flask(__name__)

    @app.errorhandler(ValueError)
    def handle_value_error(err):
        return jsonify({'error': str(err)}), 400

    @app.get('/webapp')
    def webapp_page():
        file_path = os.path.join(os.path.dirname(__file__), 'webapp.html')
        return send_file(file_path)

    @app.post('/api/me')
    def api_me():
        payload = request.get_json(silent=True) or {}
        user = _require_webapp_user(get_db, payload.get('initData', ''), token)
        return jsonify(
            {
                **user,
                'is_admin': is_admin(user['telegram_id']),
                'stats': _get_webapp_stats(get_db, user['telegram_id']),
            }
        )

    @app.get('/api/leads')
    def api_leads():
        user = _require_webapp_user(get_db, request.args.get('initData', ''), token)
        leads = _fetch_leads_for_user(
            get_db=get_db,
            user_id=user['telegram_id'],
            include_all=is_admin(user['telegram_id']),
        )
        return jsonify({'leads': leads})

    @app.post('/api/leads')
    def api_create_lead():
        payload = request.get_json(silent=True) or {}
        user = _require_webapp_user(get_db, payload.get('initData', ''), token)
        fio, city = _normalize_courier_input(payload.get('text', ''))

        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''INSERT INTO webapp_leads(owner_telegram_id, fio, city, status, orders, reward, created_at)
               VALUES (?, ?, ?, 'pending', 0, 0, ?)''',
            (user['telegram_id'], fio, city, datetime.now().strftime('%d.%m.%Y %H:%M')),
        )
        conn.commit()
        return jsonify({'ok': True})

    @app.patch('/api/leads/<int:lead_id>/status')
    def api_change_status(lead_id):
        payload = request.get_json(silent=True) or {}
        user = _require_webapp_user(get_db, payload.get('initData', ''), token)
        if not is_admin(user['telegram_id']):
            return jsonify({'error': 'forbidden'}), 403

        status = payload.get('status', 'pending')
        if status not in ('pending', 'accepted', 'rejected'):
            return jsonify({'error': 'bad status'}), 400

        conn = get_db()
        c = conn.cursor()
        c.execute('UPDATE webapp_leads SET status = ? WHERE id = ?', (status, lead_id))
        conn.commit()
        return jsonify({'ok': True})

    return app


def start_webapp_server(get_db, is_admin, token, logger, host='0.0.0.0', port=8080, webapp_url=None):
    app = build_webapp(get_db=get_db, is_admin=is_admin, token=token)
    thread = threading.Thread(
        target=lambda: app.run(host=host, port=port, debug=False, use_reloader=False),
        daemon=True,
    )
    thread.start()
    if logger:
        logger.info(f"🌐 WebApp запущен: {webapp_url or f'http://{host}:{port}/webapp'}")
    return thread
