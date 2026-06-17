FROM eclipse-temurin:21-jdk-alpine
# creating a non-root user to run the app for better security.
RUN addgroup -S spring && adduser -S spring -G spring
WORKDIR /app
VOLUME /tmp
COPY target/mlm-1.0.0.jar app.jar
EXPOSE 8080
USER spring:spring
ENTRYPOINT ["java", "-jar", "app.jar"]
# docker build -t mlm-app .
# docker run -d -p 8080:8080 --name mlm-server mlm-app