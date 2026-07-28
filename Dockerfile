FROM python:3.12-slim

WORKDIR /app

# Install Python deps first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    # Playwright installs Chromium + all required system libs in one step
    && playwright install --with-deps chromium

# Copy application source
COPY apps/       apps/
COPY supportal/  supportal/
COPY tools/      tools/
COPY public/     public/
COPY run_strabo.py run_corax.py run_cursus.py run_mcp.py run_unified.py ./

# Settings and cookies are stored in /root at runtime — mount a volume
# so they survive container restarts (see docker-compose.yml)
ENV SUPPORTAL_COOKIE=""
ENV CHAINLIT_AUTH_SECRET=""
ENV CHAINLIT_HOST="0.0.0.0"
ENV STRABO_OPEN_BROWSER="0"
# Serve all four surfaces inside the container. docker-compose also sets these;
# defaulting them here means `docker run` works without extra flags too.
ENV RUN_UNIFIED="1"
ENV MCP_TRANSPORT="sse"

EXPOSE 8765 8766 8767 8768

# run_cursus.py supervises Strabo (:8765), Corax (:8766), Cursus Unified
# (:8767, RUN_UNIFIED=1) and the MCP SSE server (:8768, MCP_TRANSPORT=sse),
# forwarding each one's stdout with a coloured label. --no-watch: hot-reload
# is pointless in an image built from a frozen source copy.
CMD ["python", "run_cursus.py", "--no-watch"]
