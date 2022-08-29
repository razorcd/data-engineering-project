# docker build -t dbt_transform .

# docker run --rm -it \          
#     -v $PWD:/dbt \
#     -v /home/cristiandugacicu/.google/credentials/google_credentials.json:/dbt/google_credentials.json \
#     -v profiles.yml:/root/.dbt/profiles.yml \
#     dbt_transform

FROM python:3.6.5
# FROM davidgasquez/dbt:latest

# Update and install system packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev python-dev cron && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install DBT
RUN pip install -U pip
RUN pip install dbt==0.17.2

# Set environment variables
ENV DBT_DIR /dbt

# Set working directory
WORKDIR $DBT_DIR

# RUN echo * * * * * echo -$(pwd)- >> 1.txt >> /etc/crontab

# Run dbt
CMD ["bash", "-i", "start.sh"]
