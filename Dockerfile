FROM node:20

# Install Python and build dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files and install JS dependencies
COPY package.json yarn.lock* package-lock.json* ./
RUN yarn install

# Copy the rest of the application
COPY . .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r scripts/requirements.txt --break-system-packages

EXPOSE 3000

ENV NODE_ENV production

# The command is overridden by docker-compose for different services
CMD ["yarn", "start"]
