export default {
  vespaMaxRetryAttempts: 8,
  vespaRetryDelay: 1000, // 1 sec
  vespaMaxRetryDelay: 30000, // 30 sec
  vespaRetryJitter: 0.25,
  vespaBaseHost: "0.0.0.0",
  page: 8,
  isDebugMode: false,
  userQueryUpdateInterval: 60 * 1000, // 1 minute,
  namespace: "namespace",
  cluster: "my_content",
  productionServerUrl: "",
  apiKey: "",
  feedEndpoint: "http://0.0.0.0:8080",
  queryEndpoint: "http://0.0.0.0:8081",
}
