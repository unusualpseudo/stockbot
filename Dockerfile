# Use the Spark image as a base
FROM spark:3.5.3-scala2.12-java17-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt


VOLUME /app/data
VOLUME /app/logs

# Command to run the Python script and redirect output to a log file with a timestamp
CMD ["sh", "-c", "python3 ./app/fundamental_analysis.py > /app/logs/fundamental_analysis_$(date +'%Y%m%d_%H%M%S').log 2>&1"]
