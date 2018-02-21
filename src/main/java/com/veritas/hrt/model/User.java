package com.veritas.hrt.model;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class User {

    private int id;
    private String login;
    private String password;
    private String email;
    private Status status;
    private LinkedList<Permission> permissions;

    public User() {
        this.id = 0;
        this.login = "login";
        this.password = "password";
        this.email = "email";
        this.status = Status.ACTIVE;
        this.permissions = new LinkedList<>();
    }

    public User(int id, String login, String password, String email, Status status, LinkedList<Permission> permissions) {
        this.id = id;
        this.login = login;
        this.password = password;
        this.email = email;
        this.status = status;
        this.permissions = permissions;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<Permission> getPermissions() {
        return permissions;
    }

    public void setPermissions(Collection<Permission> permissions) {
        this.permissions.addAll(permissions);
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", login='" + login + '\'' +
                ", password='" + password + '\'' +
                ", email='" + email + '\'' +
                ", status=" + status +
                ", permissions=" + permissions +
                '}';
    }

}
