package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory;
import io.digdag.standards.operator.aws.AWSSessionCredentialsFactory.AcceptableUri;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.NoTransactionHelper;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;

abstract class BaseRedshiftLoadOperator<T extends RedshiftConnection.StatementConfig>
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
{
    private static final String QUERY_ID = "queryId";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @VisibleForTesting
    BaseRedshiftLoadOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine)
    {
        super(systemConfig, context, templateEngine);
    }

    /* TODO: This method name should be connectionConfig() or something? */
    @Override
    protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
    {
        return RedshiftConnectionConfig.configure(secrets, params);
    }

    /* TODO: This method should be in XxxxConnectionConfig ? */
    @Override
    protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig)
    {
        return RedshiftConnection.open(connectionConfig,false);
    }

    /* TODO: This method should be in XxxxConnectionConfig ? */
    @Override
    protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig,boolean debug)
    {
        return RedshiftConnection.open(connectionConfig,debug);
    }

    @Override
    protected SecretProvider getSecretsForConnectionConfig()
    {
        return context.getSecrets().getSecrets("aws.redshift");
    }

    protected abstract List<SecretProvider> additionalSecretProvidersForCredentials(SecretProvider awsSecrets);

    private String getSecretValue(SecretProvider secretProvider, String key)
    {
        SecretProvider awsSecrets = secretProvider.getSecrets("aws");
        SecretProvider redshiftSecrets = awsSecrets.getSecrets("redshift");
        List<SecretProvider> secretProviders = additionalSecretProvidersForCredentials(awsSecrets);

        return secretProviders.stream()
                .map(sp -> sp.getSecretOptional(key))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.absent())
                .or(redshiftSecrets.getSecretOptional(key))
                .or(() -> awsSecrets.getSecret(key));
    }

    private Optional<String> getSecretOptionalValue(SecretProvider secretProvider, String key)
    {
        SecretProvider awsSecrets = secretProvider.getSecrets("aws");
        SecretProvider redshiftSecrets = awsSecrets.getSecrets("redshift");
        List<SecretProvider> secretProviders = additionalSecretProvidersForCredentials(awsSecrets);

        return secretProviders.stream()
                .map(sp -> sp.getSecretOptional(key))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.absent())
                .or(redshiftSecrets.getSecretOptional(key))
                .or(awsSecrets.getSecretOptional(key));
    }

    private BasicAWSCredentials createBaseCredential(SecretProvider secretProvider)
    {
        return new BasicAWSCredentials(
                getSecretValue(secretProvider, "access_key_id"),
                getSecretValue(secretProvider, "secret_access_key"));
    }

    private AWSSessionCredentials createSessionCredentials(Config config, SecretProvider secrets, BasicAWSCredentials baseCredential)
    {
        List<AcceptableUri> acceptableUris = buildAcceptableUriForSessionCredentials(config, baseCredential);

        if (!config.get("temp_credentials", Boolean.class, true)) {
            return new BasicSessionCredentials(
                    baseCredential.getAWSAccessKeyId(),
                    baseCredential.getAWSSecretKey(),
                    null
            );
        }

        AWSSessionCredentialsFactory sessionCredentialsFactory =
                new AWSSessionCredentialsFactory(baseCredential, acceptableUris);

        Optional<String> roleArn = getSecretOptionalValue(secrets, "role_arn");
        if (roleArn.isPresent()) {
            sessionCredentialsFactory.withRoleArn(roleArn.get());
            Optional<String> roleSessionName = secrets.getSecretOptional("role_session_name");
            if (roleSessionName.isPresent()) {
                sessionCredentialsFactory.withRoleSessionName(roleSessionName.get());
            }
        }

        Optional<Integer> durationSeconds = config.getOptional("session_duration", Integer.class);
        if (durationSeconds.isPresent()) {
            sessionCredentialsFactory.withDurationSeconds(durationSeconds.get());
        }

        return sessionCredentialsFactory.get();
    }

    protected abstract List<AcceptableUri> buildAcceptableUriForSessionCredentials(Config config, AWSCredentials baseCredential);

    protected abstract T createStatementConfig(Config params, AWSSessionCredentials sessionCredentials, String queryId);

    protected abstract String buildSQLStatement(RedshiftConnection connection, T statementConfig, boolean maskCredentials);

    protected abstract void beforeConnect(AWSCredentials credentials, T statemenetConfig);

    @Override
    protected TaskResult run(Config params, Config state, RedshiftConnectionConfig connectionConfig)
    {
        UUID queryId;
        // generate query id
        if (!state.has(QUERY_ID)) {
            // this is the first execution of this task
            logger.debug("Generating query id for a new {} task", type());
            queryId = UUID.randomUUID();
            state.set(QUERY_ID, queryId);
            throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
        }
        queryId = state.get(QUERY_ID, UUID.class);

        BasicAWSCredentials baseCredentials = createBaseCredential(context.getSecrets());
        AWSSessionCredentials sessionCredentials = createSessionCredentials(params, context.getSecrets(), baseCredentials);
        T statementConfig = createStatementConfig(params, sessionCredentials, queryId.toString());

        beforeConnect(baseCredentials, statementConfig);

        pollingRetryExecutor(TaskState.of(state), "load")
                .retryIf(LockConflictException.class, x -> true)
                .withErrorMessage("Redshift Load/Unload operation failed")
                .runAction(s -> executeTask(params, connectionConfig, statementConfig, queryId));

        return TaskResult.defaultBuilder(request).build();
    }

    private void executeTask(Config params, RedshiftConnectionConfig connectionConfig, T statementConfig, UUID queryId)
            throws LockConflictException
    {
        boolean strictTransaction = strictTransaction(params);

        try (RedshiftConnection connection = connect(connectionConfig,false)) {
            String query = buildSQLStatement(connection, statementConfig, false);

            Exception statementError = connection.validateStatement(query);
            if (statementError != null) {
                statementConfig.accessKeyId = "********";
                statementConfig.secretAccessKey = "********";
                String queryForLogging = buildSQLStatement(connection, statementConfig, true);
                throw new ConfigException("Given query is invalid: " + queryForLogging, statementError);
            }

            TransactionHelper txHelper;
            if (strictTransaction) {
                txHelper = connection.getStrictTransactionHelper(
                        statusTableSchema, statusTableName, statusTableCleanupDuration.getDuration());
            }
            else {
                txHelper = new NoTransactionHelper();
            }

            txHelper.prepare(queryId);

            boolean executed = txHelper.lockedTransaction(queryId, () -> {
                connection.executeUpdate(query);
            });

            if (!executed) {
                logger.debug("Query is already completed according to status table. Skipping statement execution.");
            }

            try {
                txHelper.cleanup();
            }
            catch (Exception ex) {
                logger.warn("Error during cleaning up status table. Ignoring.", ex);
            }
        }
        catch (DatabaseException ex) {
            // expected error that should suppress stacktrace by default
            String message = String.format("%s [%s]", ex.getMessage(), ex.getCause().getMessage());
            throw new TaskExecutionException(message, ex);
        }
    }
}
