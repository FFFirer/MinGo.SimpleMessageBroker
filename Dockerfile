FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["src/SimpleMessageBroker.Server/SimpleMessageBroker.Server.csproj", "SimpleMessageBroker.Server/"]
COPY ["src/SimpleMessageBroker.Client/SimpleMessageBroker.Client.csproj", "SimpleMessageBroker.Client/"]
RUN dotnet restore "SimpleMessageBroker.Server/SimpleMessageBroker.Server.csproj"

COPY . .
RUN dotnet build "SimpleMessageBroker.Server/SimpleMessageBroker.Server.csproj" \
    -c "$BUILD_CONFIGURATION" -o /app/build

FROM build AS publish
RUN dotnet publish "SimpleMessageBroker.Server/SimpleMessageBroker.Server.csproj" \
    -c "$BUILD_CONFIGURATION" -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SimpleMessageBroker.Server.dll"]
