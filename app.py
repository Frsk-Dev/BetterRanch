import os
import secrets
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from flask import Flask, abort, redirect, render_template, request, session, url_for

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")

DISCORD_CLIENT_ID     = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI  = os.getenv("DISCORD_REDIRECT_URI", "http://localhost:5000/callback")

_API      = "https://discord.com/api/v10"
_AUTH_URL = "https://discord.com/oauth2/authorize"
_TOKEN_URL = f"{_API}/oauth2/token"
_SCOPES   = "identify guilds"


def _avatar_url(user: dict) -> str:
    """Return the user's Discord avatar URL, or a default if they have none."""
    if user.get("avatar"):
        return f"https://cdn.discordapp.com/avatars/{user['id']}/{user['avatar']}.png"
    default_index = (int(user["id"]) >> 22) % 6
    return f"https://cdn.discordapp.com/embed/avatars/{default_index}.png"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", user=session.get("user"))


@app.route("/login")
def login():
    state = secrets.token_urlsafe(16)
    session["oauth_state"] = state
    params = {
        "client_id":     DISCORD_CLIENT_ID,
        "redirect_uri":  DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope":         _SCOPES,
        "state":         state,
    }
    return redirect(f"{_AUTH_URL}?{urlencode(params)}")


@app.route("/callback")
def callback():
    # Verify CSRF state token.
    if request.args.get("state") != session.pop("oauth_state", None):
        abort(403)

    code = request.args.get("code")
    if not code:
        abort(400)

    # Exchange authorisation code for access token.
    token_resp = requests.post(
        _TOKEN_URL,
        data={
            "client_id":     DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type":    "authorization_code",
            "code":          code,
            "redirect_uri":  DISCORD_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    # Fetch the authenticated user's profile.
    user_resp = requests.get(
        f"{_API}/users/@me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    user_resp.raise_for_status()
    user = user_resp.json()

    session["user"] = {
        "id":         user["id"],
        "username":   user["username"],
        "avatar_url": _avatar_url(user),
    }
    session["access_token"] = access_token

    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html", user=session["user"])


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True, port=5000)
