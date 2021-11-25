# syntax=docker/dockerfile:1
FROM python:3.8.7-alpine
COPY scraper_root /scraper/scraper_root
RUN apk add --no-cache gcc musl-dev linux-headers libffi-dev g++
RUN pip install --upgrade pip
RUN pip install -r /scraper/scraper_root/requirements.txt
COPY config*.json /scraper/
WORKDIR /scraper
ENV PYTHONPATH "${PYTHONPATH}:/scraper"
CMD ["python3", "scraper_root/scraper.py"]