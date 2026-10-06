# Bot-news

Telegram news bot with batch AI ranking, a per-day delivery limit, article
summaries, vote collection, and an adaptive recommendation profile.

## AI provider

AI requests use the OpenRouter OpenAI-compatible Chat Completions API. Create an
API key at [OpenRouter](https://openrouter.ai/keys), then set
`OPENROUTER_API_KEY` in the bot's environment file. Do not commit or share the
key. `OPENROUTER_MODEL` defaults to
`nvidia/nemotron-3.5-lightning:free`.

The free Nemotron endpoint does not accept `response_format`, so the bot asks for
JSON in the prompt and parses plain JSON, fenced JSON, or an object surrounded by
extra text. The request disables reasoning with OpenRouter's `reasoning.enabled`
setting and uses separate output limits: `OPENROUTER_RANKING_MAX_TOKENS=8192`
for a batch and `OPENROUTER_PROFILE_MAX_TOKENS=2048` for a profile update. Both
settings can be adjusted in the environment file.

Copy `.env.example` to your environment file and set at least `BOT_TOKEN`,
`CHAT_ID`, and `OPENROUTER_API_KEY`. Existing batch, daily limit, and profile
settings can be left at their documented defaults. Install the existing
requirements with `pip install -r requirements.txt`; no additional packages are
needed for the OpenRouter integration.
