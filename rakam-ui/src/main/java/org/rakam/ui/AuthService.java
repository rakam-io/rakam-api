package org.rakam.ui;

public interface AuthService {
    boolean login(String username, String password);

    void checkAccess(int userId);
}
