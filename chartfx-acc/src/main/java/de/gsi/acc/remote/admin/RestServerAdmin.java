package de.gsi.acc.remote.admin;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.post;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.gsi.acc.remote.BasicRestRoles;
import de.gsi.acc.remote.RestServer;
import de.gsi.acc.remote.login.LoginController;
import de.gsi.acc.remote.user.RestUserHandler;
import de.gsi.acc.remote.util.MessageBundle;

import io.javalin.core.security.Role;
import io.javalin.http.Handler;

/**
 * Basic ResetServer admin interface
 * @author rstein
 */
@SuppressWarnings("PMD.FieldNamingConventions")
public class RestServerAdmin { // NOPMD - nomen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(RestServerAdmin.class);
    private static final String ENDPOINT_ADMIN = "/admin";
    private static final String TEMPLATE_ADMIN = "/velocity/admin/admin.vm";

    private static final Handler serveAdminPage = ctx -> {
        final String userName = LoginController.getSessionCurrentUser(ctx);
        final Set<Role> roles = LoginController.getSessionCurrentRoles(ctx);
        if (!roles.contains(BasicRestRoles.ADMIN)) {
            LOGGER.atWarn().addArgument(userName).log("user '{}' does not have the required admin access rights");
            ctx.res.sendError(401);
            return;
        }
        RestUserHandler userHandler = RestServer.getUserHandler();
        final Map<String, Object> model = MessageBundle.baseModel(ctx);
        model.put("userHandler", userHandler);
        model.put("users", userHandler.getAllUserNames());
        model.put("endpoints", RestServer.getEndpoints());

        ctx.render(TEMPLATE_ADMIN, model);
    };

    private static final Handler handleAdminPost = ctx -> {
        final String userName = LoginController.getSessionCurrentUser(ctx);
        final Set<Role> roles = LoginController.getSessionCurrentRoles(ctx);
        if (!roles.contains(BasicRestRoles.ADMIN)) {
            LOGGER.atWarn().addArgument(userName).log("user '{}' does not have the required admin access rights");
            ctx.res.sendError(401);
            return;
        }
        final Map<String, Object> model = MessageBundle.baseModel(ctx);

        // parse and process admin stuff
        ctx.render(TEMPLATE_ADMIN, model);
    };

    /**
     * registers the login/logout and locale change listener
     */
    public static void register() {
        RestServer.getInstance().routes(() -> {
            post(ENDPOINT_ADMIN, handleAdminPost, Collections.singleton(BasicRestRoles.ADMIN));
            get(ENDPOINT_ADMIN, serveAdminPage, Collections.singleton(BasicRestRoles.ADMIN));
        });
    }
}
