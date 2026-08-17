FROM elixir:1.20.5-otp-29

RUN useradd --create-home app
USER app
WORKDIR /home/app/cache

COPY --chown=app:app mix.exs .formatter.exs .tool-versions ./
COPY --chown=app:app lib lib
COPY --chown=app:app test test

RUN mix compile --warnings-as-errors && MIX_ENV=test mix compile --warnings-as-errors

CMD ["mix", "check"]
