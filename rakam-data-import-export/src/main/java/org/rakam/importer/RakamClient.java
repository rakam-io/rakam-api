package org.rakam.importer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import org.rakam.plugin.user.User;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.util.JsonHelper;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class RakamClient {

    private final String url;

    public RakamClient(String url) {
        this.url = url;
    }

    public static class UserList {
        public final String project;
        public final List<User> users;

        public UserList(String project, List<User> users) {
            this.project = project;
            this.users = users;
        }
    }

    public void setUserProperty(UserList users) {
        HttpURLConnection con = null;
        try {
            URL obj = new URL(url+"/user/batch/create");
            con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");

            String urlParameters = JsonHelper.encode(users);

            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.write(urlParameters.getBytes("UTF-8"));

            wr.flush();
            wr.close();
            if(con.getResponseCode() != 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
            }
        } catch (IOException e) {
            if(con != null) {
                try {
                    throw new RuntimeException("Couldn't upload user data to Rakam: " + con.getResponseMessage(), e);
                } catch (IOException e1) {
                    throw Throwables.propagate(e);
                }
            }else {
                throw Throwables.propagate(e);
            }
        }
    }

    public void batchEvents(EventList eventList) {
        HttpURLConnection con = null;
        try {
            URL obj = new URL(url+"/event/batch");
            con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");

            String urlParameters = JsonHelper.encode(eventList);

            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.write(urlParameters.getBytes("UTF-8"));

            wr.flush();
            wr.close();
            if(con.getResponseCode() != 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
            }
        } catch (IOException e) {
            if(con != null) {
                try {
                    throw new RuntimeException("Couldn't upload events to Rakam: " + con.getResponseMessage(), e);
                } catch (IOException e1) {
                    throw Throwables.propagate(e);
                }
            }else {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class Event {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String project;
        public String collection;
        public Map<String, Object> properties;

        @JsonCreator
        public Event(@JsonProperty("project") String project,
                     @JsonProperty("collection") String collection,
                     @JsonProperty("properties") Map<String, Object> properties) {
            this.project = project;
            this.collection = collection;
            this.properties = properties;
        }
    }

    public static class EventList {
        public final org.rakam.collection.Event.EventContext api;
        public final String project;
        public final List<Event> events;

        @JsonCreator
        public EventList(@ApiParam(name = "api") org.rakam.collection.Event.EventContext context,
                         @ApiParam(name = "project") String project,
                         @ApiParam(name = "events") List<Event> events) {
            this.project = project;
            this.events = events;
            this.api = checkNotNull(context, "api is null");
        }
    }
}
