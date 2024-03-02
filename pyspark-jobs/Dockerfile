FROM bde2020/spark-python-template:3.0.0-hadoop3.2-java11

# COPY hello-world.py /app/hello-world.py

# ONBUILD COPY requirements.txt /app
# ONBUILD RUN cd /app && pip3 install -r requirements.txt

# ONBUILD COPY . /app
	  
# ENV SPARK_APPLICATION_PYTHON_LOCATION /app/hello-world.py
# ENV SPARK_APPLICATION_ARGS "foo bar baz"

# Install Chrome and WebDriver for Selenium
RUN apt-get update && apt-get install -y wget gnupg unzip \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && wget https://chromedriver.storage.googleapis.com/91.0.4472.19/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/bin \
    && chmod +x /usr/bin/chromedriver \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the content of the local src directory to the working directory
COPY . .

# Command to run on container start
CMD [ "python", "./flight-selenium.py" ]