# Dockerfiles
```Dockerfile
FROM eclipse-temurin:21-jdk-alpine

VOLUME /tmp

# Copy the file but rename it to 'mlm.jar' (stripping the version is usually safer)
COPY target/mlm-1.0.0.jar mlm.jar

# Update the command to match the new name
ENTRYPOINT ["java", "-jar", "/mlm.jar"]
```
Then Run
```bash
# First build the image
docker build -t mlm-app .
# Then -d is detached mode (background)
docker run -d -p 8080:8080 --name mlm-server mlm-app .
```
### Common Commands Cheat Sheet
Stop the container: Ctrl+C (if running in foreground) or `docker stop <container_id>`.
Run in background: Add -d to the run command: `docker run -d -p 8080:8080 my-spring-app`.
See running containers: `docker ps`.

# Code as Infrastructure
## Docker as a Development Environment
Make changes to your user settings json and you can bring extensions into any environment with you.
```json
{
    "workbench.colorTheme": "Dracula",
    // ... other settings ...

    // ADD THIS BLOCK:
    "dev.containers.defaultExtensions": [
        "eamodio.gitlens",       // I want GitLens everywhere
        "esbenp.prettier-vscode",// I want Prettier everywhere
        "coenraads.bracket-pair-colorizer" 
    ]
}
```
### devcontainer/devcontainer.json
You can configure an environment using the devcontainer.json file.
```json
{
    "name": "Spring Boot Dev Container",
    "image": "mcr.microsoft.com/vscode/devcontainers/java:21-jdk",
    "features": {
        "ghcr.io/devcontainers/features/docker-in-docker:1": {}
    },
    "postCreateCommand": "./mvnw clean install",
    "forwardPorts": [8080],
    "remoteUser": "vscode"
}
```

## Deploy a docker prod image
### Build the Production Image
```shell
# This tells docker to build an image and container using this compose file
docker compose -f docker-compose-prod.yml up --build
# This triggers the "Multi-Stage Build" in your Dockerfile
docker build -t mlm-production:v1 .

# Alternatively you can use no cache to ensure a fresh build
docker build --no-cache -t mlm-production:v1 .

# 2. Check the size (Should be roughly 200MB - 300MB total)
docker images mlm-production:v1

# 3. Try to find Maven inside it (This will fail, proving it's gone)
docker run --rm mlm-production:v1 which mvn
# Output: null / no such file (This is what you want!)
```

### Transfer a file out of the container
```bash
docker cp mlm-dev-container:/app/target/mlm-1.0.0.jar ./my-local-copy.jar
```




## Deployment to AWS
Here is the high-level architecture of an AWS deployment:

You need three specific things to make this happen on AWS.

1. The Warehouse: **Amazon ECR (Elastic Container Registry)**
You cannot drag-and-drop a Docker image into the AWS console. You need a place to store it.

     - What it is: A private "App Store" for your own code.

     - The Workflow: You will run a command like docker push on your laptop. Your massive image (layers of Java, Linux, and your App) travels over the internet and sits in ECR, waiting to be used.

2. The Runner: **AWS App Runner** (Recommended for you)
Since you are a single developer deploying a Spring Boot app, AWS App Runner is the modern "Golden Path."

    - Why: It connects directly to ECR. You basically say: "Hey AWS, see that image? Run it. I don't care about servers, load balancers, or VPCs. just give me a URL."

    - Alternative **ECS Fargate**: This is the industry standard for complex setups. It’s more like "Lego blocks" where you configure the networking, security groups, and clusters yourself. For your first deploy, App Runner is 10x easier.

3. The Artifact: The **Production Image**
Crucial Distinction: You currently have two "recipes" in your project.

devcontainer.json: This is for coding (contains Git, Maven, VS Code server). DO NOT DEPLOY THIS.

Dockerfile: This is for shipping (the one we made earlier with USER spring). DEPLOY THIS.

## The Step-by-Step "Manual" Deployment
Here is exactly how you do this from your terminal right now.

### Phase 1: Create the Warehouse (ECR)
1. Log into the **AWS Console**.

2. Go to **Elastic Container Registry (ECR)**.

3. Click Create repository.

4. Name it mlm-app and hit Create.

5. Keep this window open; it gives you the specific "Push Commands" for the next step.

### Phase 2: Push the Image (From your HomeLab)
You need the AWS CLI installed for this, but here is the logic:

1. Login: You tell your local Docker "It's okay to talk to AWS."

2. PowerShell
```powershell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin [YOUR_ACCOUNT_ID].dkr.ecr.us-east-1.amazonaws.com
```
3. Tag: You put a sticker on your local image with the AWS address.
```powershell
docker tag mlm-app:latest [YOUR_ACCOUNT_ID].dkr.ecr.us-east-1.amazonaws.com/mlm-app:latest
```
4. Push: You upload it.
```powershell
docker push [YOUR_ACCOUNT_ID].dkr.ecr.us-east-1.amazonaws.com/mlm-app:latest
```
### Phase 3: Turn it On (App Runner)
1. Go to AWS App Runner in the console.

2. Click Create Service.

3. Source: Select Container Registry.

4. Image URI: Browse and pick the image you just pushed to ECR.

5. **Settings** (The important part):

6. Port: 8080 (This MUST match your Dockerfile EXPOSE).

7. Environment Variables: Add any DB passwords here.

8. Click Deploy.
   
9. In about 3–5 minutes, AWS will give you a public URL (e.g., https://random-name.awsapprunner.com).





## Layered Jars
While it is possible to convert a Spring Boot **uber jar** into a **Docker image** with just a few lines in the **Dockerfile**, using the layering feature will result in an optimized image. When you create a jar containing the **layers index file**, the `spring-boot-jarmode-tools` jar will be added as a dependency to your jar. With this jar on the classpath, you can launch your application in a special mode which allows the bootstrap code to run something entirely different from your application, for example, something that extracts the layers.

Here’s how you can launch your jar with a tools jar mode:
```shell
java -Djarmode=layertools -jar myapp.jar extract
# Which outputs:
Usage:
  java -Djarmode=tools -jar my-app.jar

Available commands:
  extract      Extract the contents from the jar
  list-layers  List layers from the jar that can be extracted
  help         Help about any command
```
You can specify the location 
```bash
docker build --build-arg JAR_FILE=path/to/myapp.jar .
```

# Docker Compose
is a popular technology that can be used to define and manage **multiple** containers for services that your application needs. A compose.yml file is typically created next to your application which defines and configures service containers.
Docker Compose is to run **docker compose up**, work on your application with it connecting to started services, then run **docker compose down** when you are finished.

Maven POM
```xml
<dependencies>
	<dependency>
		<groupId>org.springframework.boot</groupId>
		<artifactId>spring-boot-docker-compose</artifactId>
		<optional>true</optional>
	</dependency>
</dependencies>
```

### Referencing a Specific Container file by path
If your compose file is not in the same directory as your application, or if it’s named differently, you can use `spring.docker.compose.file` in your `application.properties` or `application.yaml` to point to a different file. Properties can be defined as an exact path or a path that’s relative to your application.

To enable SSL support for a service, you can use service labels.

### For JKS based keystores and truststores, you can use the following container labels:
- org.springframework.boot.sslbundle.jks.key.alias
- org.springframework.boot.sslbundle.jks.key.password
- org.springframework.boot.sslbundle.jks.options.ciphers
- org.springframework.boot.sslbundle.jks.options.enabled-protocols
- org.springframework.boot.sslbundle.jks.protocol
- org.springframework.boot.sslbundle.jks.keystore.type
- org.springframework.boot.sslbundle.jks.keystore.provider
- org.springframework.boot.sslbundle.jks.keystore.location
- org.springframework.boot.sslbundle.jks.keystore.password
- org.springframework.boot.sslbundle.jks.truststore.type
- org.springframework.boot.sslbundle.jks.truststore.provider
- org.springframework.boot.sslbundle.jks.truststore.location
- org.springframework.boot.sslbundle.jks.truststore.password

### For PEM based keystores and truststores, you can use the following container labels:
- org.springframework.boot.sslbundle.pem.key.alias
- org.springframework.boot.sslbundle.pem.key.password
- org.springframework.boot.sslbundle.pem.options.ciphers
- org.springframework.boot.sslbundle.pem.options.enabled-protocols
- org.springframework.boot.sslbundle.pem.protocol
- org.springframework.boot.sslbundle.pem.keystore.type
- org.springframework.boot.sslbundle.pem.keystore.certificate
- org.springframework.boot.sslbundle.pem.keystore.private-key
- org.springframework.boot.sslbundle.pem.keystore.private-key-password
- org.springframework.boot.sslbundle.pem.truststore.type
- org.springframework.boot.sslbundle.pem.truststore.certificate
- org.springframework.boot.sslbundle.pem.truststore.private-key
- org.springframework.boot.sslbundle.pem.truststore.private-key-password


This configuration starts a Redis service with TLS enabled, using the provided SSL certificates and keys.
```yaml
services:
  redis:
    image: 'redis:latest'
    ports:
      - '6379'
    secrets:
      - ssl-ca
      - ssl-key
      - ssl-cert
    command: 'redis-server --tls-port 6379 --port 0 --tls-cert-file /run/secrets/ssl-cert --tls-key-file /run/secrets/ssl-key --tls-ca-cert-file /run/secrets/ssl-ca'
    labels:
      - 'org.springframework.boot.sslbundle.pem.keystore.certificate=client.crt'
      - 'org.springframework.boot.sslbundle.pem.keystore.private-key=client.key'
      - 'org.springframework.boot.sslbundle.pem.truststore.certificate=ca.crt'
secrets:
  ssl-ca:
    file: 'ca.crt'
  ssl-key:
    file: 'server.key'
  ssl-cert:
    file: 'server.crt'
```

### Ignoring a Container
Any container with labeled with `org.springframework.boot.ignore: true` will be ignored by Spring Boot Docker Compose support.

### Changing Timeout Values
```properties
spring.docker.compose.readiness.tcp.connect-timeout=10s
spring.docker.compose.readiness.tcp.read-timeout=5s
```
```yaml
spring:
  docker:
    compose:
      readiness:
        tcp:
          connect-timeout: 10s
          read-timeout: 5s
```


### Controlling the Docker Compose Lifecycle
By default Spring Boot calls docker compose up when your application starts and docker compose stop when it’s shut down. If you prefer to have different lifecycle management you can use the spring.docker.compose.lifecycle-management property.

The following values are supported:
- `none` - Do not start or stop Docker Compose
- `start-only` - Start Docker Compose when the application starts and leave it running
- `start-and-stop` - Start Docker Compose when the application starts and stop it when the JVM exits

In addition you can use the `spring.docker.compose.start.command` property to change whether `docker compose up` or `docker compose start` is used. The `spring.docker.compose.stop.command` allows you to configure if `docker compose down` or `docker compose stop` is used.

The following example shows how lifecycle management can be configured:
```yaml
spring:
  docker:
    compose:
      lifecycle-management: start-and-stop
      start:
        command: start
      stop:
        command: down
        timeout: 1m
```
```properties
spring.docker.compose.lifecycle-management=start-and-stop
spring.docker.compose.start.command=start
spring.docker.compose.stop.command=down
spring.docker.compose.stop.timeout=1m
```
