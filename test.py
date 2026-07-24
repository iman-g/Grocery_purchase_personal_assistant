# # list_models.py
# from google import genai

# client = genai.Client(api_key="AIzaSyBcGAscnBotHR91cR3t5nniHxXV3gv39rg")
# for model in client.models.list():
#     print(model.name, "|", model.supported_actions)

import ssl
import certifi

# Intercept default SSL context creation to bypass the buggy Windows Certificate Store
_orig_create_default_context = ssl.create_default_context

def _patched_create_default_context(purpose=ssl.Purpose.SERVER_AUTH, *, cafile=None, capath=None, cadata=None):
    # If no custom certificates are provided, force it to use certifi 
    # instead of falling back to context.load_default_certs() which causes the ASN1 crash.
    if cafile is None and capath is None and cadata is None:
        cafile = certifi.where()
    return _orig_create_default_context(purpose=purpose, cafile=cafile, capath=capath, cadata=cadata)

ssl.create_default_context = _patched_create_default_context


import asyncio
import aiohttp
# Adjust imports based on your exact function names
from albert_heijn import get_access_token, search_product 

async def test_search():
    async with aiohttp.ClientSession() as session:
        print("Fetching token...")
        token = await get_access_token(session)
        print(f"Token received: {token[:10]}...")
        
        # Assuming search_product also requires the session and token
        results = await search_product("melk", session, token) 
        
        print(f"Found {len(results)} items.")
        if results:
            print("First item sample:", results[0])

if __name__ == "__main__":
    asyncio.run(test_search())