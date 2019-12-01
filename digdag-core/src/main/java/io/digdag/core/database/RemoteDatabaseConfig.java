package io.digdag.core.database;

import java.util.List;
import java.util.Locale;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.util.Map;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

@Value.Immutable
public interface RemoteDatabaseConfig
{
    String getUser();

    String getPassword();

    String getHost();

    Optional<Integer> getPort();

    int getLoginTimeout();

    int getSocketTimeout();

    boolean getSsl();

    String getDatabase();

    Map<String,String> getOptions();

    static ImmutableRemoteDatabaseConfig.Builder builder()
    {
        return ImmutableRemoteDatabaseConfig.builder();
    }
}
