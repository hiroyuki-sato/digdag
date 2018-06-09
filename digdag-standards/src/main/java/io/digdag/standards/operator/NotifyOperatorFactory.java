package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.Notifier;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;

public class NotifyOperatorFactory
        implements OperatorFactory
{
    private final Notifier notifier;
    private final SessionStoreManager sm;
    private final TransactionManager tm;

    @Inject
    public NotifyOperatorFactory(Notifier notifier,SessionStoreManager sm,TransactionManager tm)
    {
        this.notifier = notifier;
        this.sm = sm;
        this.tm = tm;
    }

    public String getType()
    {
        return "notify";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new NotifyOperator(context, notifier,sm,tm);
    }

    private static class NotifyOperator
            implements Operator
    {
        private final TaskRequest request;
        private final Notifier notifier;
        private final SessionStoreManager sm;
        private final TransactionManager tm;

        public NotifyOperator(OperatorContext context, Notifier notifier, SessionStoreManager sm,TransactionManager tm)
        {
            this.request = context.getTaskRequest();
            this.notifier = notifier;
            this.sm = sm;
            this.tm = tm;
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            try {
                tm.begin(() -> {
                    StoredSessionAttemptWithSession attempt = sm.getSessionStore(request.getSiteId()).getAttemptById(request.getSessionId());
                    System.out.println(attempt);
                    System.out.println(attempt.getSession());
                    return attempt;
                        }, ResourceNotFoundException.class

                );
            } catch (ResourceNotFoundException ex ){

            }


            Notification notification = Notification.builder(Instant.now(), message)
                    .siteId(request.getSiteId())
                    .projectName(request.getProjectName())
                    .projectId(request.getProjectId())
                    .workflowName(request.getWorkflowName())
                    .revision(request.getRevision().or(""))
                    .attemptId(request.getAttemptId())
                    .sessionId(request.getSessionId())
                    .taskName(request.getTaskName())
                    .timeZone(request.getTimeZone())
                    .sessionUuid(request.getSessionUuid())
                    .sessionTime(OffsetDateTime.ofInstant(request.getSessionTime(), request.getTimeZone()))
                    .build();

            try {
                notifier.sendNotification(notification);
            }
            catch (NotificationException e) {
                // notification failed
                throw new TaskExecutionException(e);
            }

            return TaskResult.empty(request);
        }
    }
}
