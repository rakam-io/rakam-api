package org.rakam.analysis;


import org.rakam.util.RakamException;

public class ProjectNotExistsException extends RakamException {
    public ProjectNotExistsException() {
        super("Project does not exist", 400);
    }
}
