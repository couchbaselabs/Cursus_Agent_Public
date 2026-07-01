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
COPY run_strabo.py run_corax.py run_cursus.py run_mcp.py ./

# Settings and cookies are stored in /root at runtime — mount a volume
# so they survive container restarts (see docker-compose.yml)
ENV SUPPORTAL_COOKIE=""
ENV CHAINLIT_AUTH_SECRET=""
ENV CHAINLIT_HOST="0.0.0.0"
ENV STRABO_OPEN_BROWSER="0"

EXPOSE 8765 8766 8768

# run_cursus.py spawns both Strabo (:8765) and Corax (:8766) and
# forwards their stdout with coloured labels.
CMD ["python", "run_cursus.py"]
