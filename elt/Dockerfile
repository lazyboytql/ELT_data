FROM python:3.10-slim

# Cài đặt wget và gnupg2 để tải và thêm key cho PostgreSQL repository
RUN apt-get update && apt-get install -y wget gnupg2

# Thêm PostgreSQL Apt repository
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update

# Cài đặt PostgreSQL client phiên bản 16
RUN apt-get install -y postgresql-client-16

COPY elt_script.py .

CMD ["python", "elt_script.py"]
