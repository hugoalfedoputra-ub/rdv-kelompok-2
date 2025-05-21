FROM apache/spark:3.5.1-scala2.12-java11-python3-r-ubuntu

USER root

# Install rsync and openssh-client
RUN apt-get update && \
    apt-get install -y rsync openssh-client && \
    rm -rf /var/lib/apt/lists/*

# Create .ivy2 directory and ensure spark user owns it
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R 185:0 /home/spark/.ivy2

# Copy the Spark Connect JAR into the image's Spark JARs directory
COPY ./jars/spark-connect_2.12-3.5.1.jar /opt/spark/jars/

# Ensure correct permissions
RUN chown spark:spark /opt/spark/jars/spark-connect_2.12-3.5.1.jar && \
    chmod 644 /opt/spark/jars/spark-connect_2.12-3.5.1.jar

# Spark user
USER 185