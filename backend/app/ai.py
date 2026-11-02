import httpx
from fastapi import HTTPException

async def generate_sql(provider: str, key: str, model: str, prompt: str, schema: str, output_tokens: int) -> str:
    system = ('You are a careful SQL assistant. Generate exactly ONE DuckDB SELECT query, no markdown, '
              'no explanations, no DDL/DML, no external file or network functions. Only use listed tables. '
              'Do not assume nonexistent columns. Schema:\n' + schema)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if provider == 'openai':
                res = await client.post('https://api.openai.com/v1/chat/completions',
                    headers={'Authorization': f'Bearer {key}'}, json={'model': model, 'temperature': 0,
                    'max_completion_tokens': output_tokens, 'messages': [{'role':'system','content':system}, {'role':'user','content':prompt}]})
                res.raise_for_status()
                return res.json()['choices'][0]['message']['content'].strip()
            if provider == 'anthropic':
                res = await client.post('https://api.anthropic.com/v1/messages',
                    headers={'x-api-key':key, 'anthropic-version':'2023-06-01'},
                    json={'model':model,'max_tokens':output_tokens,'system':system,
                          'messages':[{'role':'user','content':prompt}]})
                res.raise_for_status()
                return ''.join(block.get('text','') for block in res.json()['content']).strip()
    except (httpx.HTTPError, KeyError, IndexError, ValueError) as exc:
        raise HTTPException(502, f'AI provider request failed: {type(exc).__name__}') from None
    raise HTTPException(422, 'Unsupported provider')
