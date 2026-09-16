FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -ntp dependency:go-offline

COPY src ./src
RUN mvn -B -ntp -DskipTests package

FROM eclipse-temurin:17-jre

RUN groupadd --system retrieval && useradd --system --gid retrieval --create-home retrieval

WORKDIR /app
COPY --from=build --chown=retrieval:retrieval /workspace/target/retrieval-service-0.0.1-SNAPSHOT.jar app.jar

USER retrieval
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
