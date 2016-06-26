package com.javarush.test.level39.lesson09.big01;

import com.javarush.test.level39.lesson09.big01.query.*;

import java.io.*;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {

    private Path logDir;
    private LinkedList<File> files = new LinkedList<>();

    public LogParser(Path logDir) {
        this.logDir = logDir;
        walk(logDir.toString());
    }

    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            ips.add(log.getIp());
        }
        return ips.size();
    }

    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            ips.add(log.getIp());
        }
        return ips;
    }

    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user)) {
                ips.add(log.getIp());
            }
        }
        return ips;
    }

    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == event) {
                ips.add(log.getIp());
            }
        }
        return ips;
    }

    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        Set<String> ips = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getStatus() == status) {
                ips.add(log.getIp());
            }
        }
        return ips;
    }

    private List<Log> periodSelect(Date after, Date before) {
        List<Log> logs = new ArrayList<>();
        for (String oneLog : getAllLogs()) {
            String[] log = oneLog.split("\t");
            String ip = log[0];
            String name = log[1];
            String date = log[2];
            Event event;
            int taskNumber = -1;
            String[] eventString = log[3].split(" ");
            if (eventString.length == 2) {
                taskNumber = Integer.parseInt(eventString[1]);
            }
            event = Enum.valueOf(Event.class, eventString[0]);
            Status status = Enum.valueOf(Status.class, log[4]);
            DateFormat format = new SimpleDateFormat("dd.MM.yyyy H:mm:ss");
            try {
                Date currentDate = format.parse(date);
                if (before == null && after == null) {
                    logs.add(new Log(ip, name, currentDate, event, status, taskNumber));
                } else if (after == null) {
                    if (currentDate.getTime() <= before.getTime()) {
                        logs.add(new Log(ip, name, currentDate, event, status, taskNumber));
                    }
                } else if (before == null) {
                    if (currentDate.getTime() >= after.getTime()) {
                        logs.add(new Log(ip, name, currentDate, event, status, taskNumber));
                    }
                } else {
                    if (currentDate.getTime() >= after.getTime() && currentDate.getTime() <= before.getTime()) {
                        logs.add(new Log(ip, name, currentDate, event, status, taskNumber));
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return logs;
    }

    private List<String> getAllLogs() {
        List<String> logs = new ArrayList<>();
        for (File file : files) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                while (reader.ready()) {
                    logs.add(reader.readLine());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return logs;
    }

    private void walk(String path) {
        File root = new File(path);
        File[] list = root.listFiles();
        if (list == null) return;
        for (File f : list) {
            if (f.isDirectory()) {
                walk(f.getAbsolutePath());
            } else {
                if (f.getAbsoluteFile().toString().endsWith(".log")) {
                    files.add(f.getAbsoluteFile());
                }
            }
        }
    }

    public LinkedList<File> getFiles() {
        return files;
    }

    @Override
    public Set<String> getAllUsers() {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(null, null)) {
            users.add(log.getUser());
        }
        return users;
    }

    @Override
    public int getNumberOfUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            users.add(log.getUser());
        }
        return users.size();
    }

    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user)) {
                events.add(log.getEvent());
            }
        }
        return events.size();

    }

    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getIp().equals(ip)) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.LOGIN) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.DOWNLOAD_PLUGIN) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.WRITE_MESSAGE) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.SOLVE_TASK) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getTaskNumber() == task && log.getEvent() == Event.SOLVE_TASK) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.DONE_TASK) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        Set<String> users = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getTaskNumber() == task && log.getEvent() == Event.DONE_TASK) {
                users.add(log.getUser());
            }
        }
        return users;
    }

    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getEvent() == event) {
                dates.add(log.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getStatus() == Status.FAILED) {
                dates.add(log.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getStatus() == Status.ERROR) {
                dates.add(log.getDate());
            }
        }
        return dates;
    }

    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        SortedSet<Date> dates = new TreeSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getEvent() == Event.LOGIN) {
                dates.add(log.getDate());
            }
        }
        return dates.first();
    }

    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getTaskNumber() == task && log.getEvent() == Event.SOLVE_TASK) {
                return log.getDate();
            }
        }
        return null;
    }

    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getTaskNumber() == task && log.getEvent() == Event.DONE_TASK) {
                return log.getDate();
            }
        }
        return null;
    }

    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getEvent() == Event.WRITE_MESSAGE) {
                dates.add(log.getDate());
            }
        }
        return dates;
    }

    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        Set<Date> dates = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user) && log.getEvent() == Event.DOWNLOAD_PLUGIN) {
                dates.add(log.getDate());
            }
        }
        return dates;
    }

    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            events.add(log.getEvent());
        }
        return events.size();
    }

    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            events.add(log.getEvent());
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getIp().equals(ip)) {
                events.add(log.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getUser().equals(user)) {
                events.add(log.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getStatus() == Status.FAILED) {
                events.add(log.getEvent());
            }
        }
        return events;
    }

    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        Set<Event> events = new HashSet<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getStatus() == Status.ERROR) {
                events.add(log.getEvent());
            }
        }
        return events;
    }

    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        int count = 0;
        for (Log log : periodSelect(after, before)) {
            if (log.getTaskNumber() == task && log.getEvent() == Event.SOLVE_TASK) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        int count = 0;
        for (Log log : periodSelect(after, before)) {
            if (log.getTaskNumber() == task && log.getEvent() == Event.SOLVE_TASK && log.getStatus() == Status.OK) {
                count++;
            }
        }
        return count;
    }

    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> solvedTasksMap = new HashMap<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.SOLVE_TASK) {
                int task = log.getTaskNumber();
                if (solvedTasksMap.get(task) == null) {
                    solvedTasksMap.put(task, 1);
                } else solvedTasksMap.put(task, solvedTasksMap.get(task) + 1);
            }
        }
        return solvedTasksMap;
    }

    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        Map<Integer, Integer> doneTasksMap = new HashMap<>();
        for (Log log : periodSelect(after, before)) {
            if (log.getEvent() == Event.DONE_TASK) {
                int task = log.getTaskNumber();
                if (doneTasksMap.get(task) == null) {
                    doneTasksMap.put(task, 1);
                } else doneTasksMap.put(task, doneTasksMap.get(task) + 1);
            }
        }
        return doneTasksMap;
    }

    @Override
    public Set<Object> execute(String query) {
        String field1 = query.split(" ")[1];
        String field2 = query.split(" ")[3];
        String tempValue = query.split("=")[1].replaceFirst(" ", "").replaceAll("\"", "");
        String value = tempValue.split(" and date")[0];
        String after = tempValue.split(" and ")[1].substring(13);
        String before = tempValue.split("and ")[2];
        Set<Object> resultQuery = new HashSet<>();
        DateFormat format = new SimpleDateFormat("dd.MM.yyyy H:mm:ss", Locale.ENGLISH);
        Date dateAfter = null;
        Date dateBefore = null;
        try {
            dateAfter = format.parse(after);
            dateBefore = format.parse(before);
        } catch (ParseException e) {}
        switch (field1) {
            case "ip":
                for (Log log : periodSelect(dateAfter, dateBefore)) {
                    if (log.getField(field2).toString().equals(value)) {
                        resultQuery.add(log.getIp()); continue;
                    }
                    try {
                        Date date = format.parse(value);
                        if (log.getDate().getTime() == date.getTime()) {
                            resultQuery.add(log.getIp());
                        }
                    } catch (ParseException e) {}
                };
                break;
            case "user":
                for (Log log : periodSelect(dateAfter, dateBefore)) {
                    if (log.getField(field2).toString().equals(value)) {
                        resultQuery.add(log.getUser()); continue;
                    }
                    try {
                        Date date = format.parse(value);
                        if (log.getDate().getTime() == date.getTime()) {
                            resultQuery.add(log.getUser());
                        }
                    } catch (ParseException e) {}
                };
                break;
            case "date":
                for (Log log : periodSelect(dateAfter, dateBefore)) {
                    if (log.getField(field2).toString().equals(value)) {
                        resultQuery.add(log.getDate()); continue;
                    }
                    try {
                        Date date = format.parse(value);
                        if (log.getDate().getTime() == date.getTime()) {
                            resultQuery.add(log.getDate());
                        }
                    } catch (ParseException e) {}
                };
                break;
            case "event":
                for (Log log : periodSelect(dateAfter, dateBefore)) {
                    if (log.getField(field2).toString().equals(value)) {
                        resultQuery.add(log.getEvent()); continue;
                    }
                    try {
                        Date date = format.parse(value);
                        if (log.getDate().getTime() == date.getTime()) {
                            resultQuery.add(log.getEvent());
                        }
                    } catch (ParseException e) {}
                };
                break;
            case "status":
                for (Log log : periodSelect(dateAfter, dateBefore)) {
                    if (log.getField(field2).toString().equals(value)) {
                        resultQuery.add(log.getStatus()); continue;
                    }
                    try {
                        Date date = format.parse(value);
                        if (log.getDate().getTime() == date.getTime()) {
                            resultQuery.add(log.getStatus());
                        }
                    } catch (ParseException e) {}
                };
                break;
        }
        return resultQuery;
    }

    private class Log {
        private String ip;
        private String user;
        private Date date;
        private Event event;
        private Status status;
        int taskNumber;

        public Log(String ip, String user, Date date, Event event, Status status, int taskNumber) {
            this.ip = ip;
            this.user = user;
            this.date = date;
            this.event = event;
            this.status = status;
            this.taskNumber = taskNumber;
        }

        public Object getField(String field){
            switch (field){
                case "ip": return ip;
                case "user": return user;
                case "date": return date;
                case "event": return event;
                case "status": return status;
                default:return null;
            }
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public int getTaskNumber() {
            return taskNumber;
        }

        public void setTaskNumber(int taskNumber) {
            this.taskNumber = taskNumber;
        }

        @Override
        public String toString() {
            return "Log{" +
                    "ip='" + ip + '\'' +
                    ", user='" + user + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", status=" + status +
                    ", taskNumber=" + taskNumber +
                    '}';
        }
    }
}