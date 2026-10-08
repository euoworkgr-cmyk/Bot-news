# Bot-news

Telegram news bot with batch AI ranking, a per-day delivery limit, article
summaries, vote collection, and an adaptive recommendation profile.

## AI provider

AI requests use Polza AI's OpenAI-compatible Chat Completions API at
`https://polza.ai/api/v1`. Set `POLZA_API_KEY` in the bot's environment file;
do not commit or share the key. `POLZA_MODEL` defaults to
`openai/gpt-6-luna`.

The bot asks the model for JSON in the prompt and accepts plain JSON, fenced
JSON, or an object surrounded by extra text. GPT-6 Luna requests set
`reasoning_effort=none` for these focused ranking and profile tasks. Separate
output limits are used: `OPENROUTER_RANKING_MAX_TOKENS=8192` for a batch and
`OPENROUTER_PROFILE_MAX_TOKENS=2048` for a profile update. These existing
setting names are retained for compatibility and can be adjusted in the
environment file.

Copy `.env.example` to your environment file and set at least `BOT_TOKEN`,
`CHAT_ID`, and `POLZA_API_KEY`. Install the existing requirements with
`pip install -r requirements.txt`; no additional packages are needed for the
Polza AI integration.
