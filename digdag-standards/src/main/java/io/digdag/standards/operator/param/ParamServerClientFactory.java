package io.digdag.standards.operator.param;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;

public class ParamServerClientFactory
{
    public static ParamServerClient build(ParamServerClientConnection connection, ObjectMapper objectMapper, Config systemConfig)
    {
        switch (connection.getType()) {
            case "postgresql":
                return new PostgresqlParamServerClient(connection, objectMapper, systemConfig);
            case "redis":
                return new RedisParamServerClient(connection, objectMapper, systemConfig);
            default:
                return new DummyParamServerClient(connection, objectMapper, systemConfig);
        }
    }
}
