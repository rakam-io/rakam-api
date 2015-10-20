package org.rakam.analysis;


import io.netty.handler.codec.http.HttpResponseStatus;
import org.rakam.util.RakamException;

public class ProjectNotExistsException extends RakamException {
    public ProjectNotExistsException() {
        super("Project does not exist", HttpResponseStatus.UNAUTHORIZED);
    }
}
