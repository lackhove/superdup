FROM python:3

WORKDIR /superdup

RUN pip install --no-cache-dir attrs

#RUN wget https://github.com/gilbertchen/duplicacy/releases/download/v2.7.1/duplicacy_linux_x64_2.7.1 \
#&& mv duplicacy_linux_x64_2.7.1 /usr/local/bin/duplicacy && chmod +x /usr/local/bin/duplicacy
COPY duplicacy /usr/local/bin/duplicacy
RUN chmod +x /usr/local/bin/duplicacy

COPY superdup.py /usr/local/bin/superdup.py

CMD [ "python", "-u", "/usr/local/bin/superdup.py", "--config", "/superdup/config.ini"]
