# Colab LLM tunnel options

## Goal

Let the Qwen Colab launcher expose its local llama.cpp server through one of
three deliberate tunnel modes.  The local application must receive an
OpenAI-compatible HTTPS `/v1` base URL and continue to authenticate inference
requests with the API key configured in the launcher.

## Tunnel modes

| Mode | Prerequisites | Public address | Streaming |
| --- | --- | --- | --- |
| Cloudflare Named Tunnel | Cloudflare tunnel token and configured public hostname | Stable, user-owned hostname | Supported |
| ngrok | ngrok authtoken | ngrok HTTPS address generated per run | Supported |
| Cloudflare Quick Tunnel | None | Random `trycloudflare.com` address generated per run | Not supported |

The launcher will retain Named Tunnel as the first/default choice.  It will
prompt for exactly the secret required by the selected mode using `getpass`:
a Cloudflare tunnel token for Named Tunnel, an ngrok authtoken for ngrok, and
no tunnel secret for Quick Tunnel.  The llama.cpp API key remains mandatory in
every mode.

## Runtime flow

1. Build llama.cpp, download the model, and start the server on
   `127.0.0.1:8000` as today.
2. Confirm readiness and confirm an unauthenticated protected endpoint returns
   `401` before any tunnel starts.
3. Start the selected tunnel and obtain its public URL:
   - Named Tunnel uses the hostname supplied by the user because its URL is
     configured in Cloudflare.
   - ngrok starts an HTTP tunnel and reads the HTTPS URL from ngrok's local
     inspection API.
   - Quick Tunnel reads the generated HTTPS URL from cloudflared output.
4. Print the exact `LLM_BASE_URL`, `LLM_MODEL`, and `LLM_API_KEY` values needed
   locally, then keep both child processes alive until the Colab cell is
   interrupted.

Tunnel start-up has a bounded wait and reports a useful error if no HTTPS URL
is available.  Secrets are never included in printable shell commands or in
the generated profile.

## Streaming and safety

Cloudflare Quick Tunnels are temporary development endpoints and do not
support Server-Sent Events.  The launcher will emit a conspicuous warning and
the README will require callers to send `stream: false` in this mode.  ngrok
and Named Tunnels are the documented choices for the project's streaming
frontend.  All modes keep llama.cpp bound to loopback and require its API key
for protected inference routes.

## Testing and documentation

Unit tests will cover tunnel command construction, generated URL normalization
and extraction, and the mode-specific configuration rules without opening real
tunnels.  Existing authentication tests remain intact.  The README will add a
comparison table, step-by-step ngrok instructions, and a no-public-hostname
Quick Tunnel section including its non-streaming restriction.
