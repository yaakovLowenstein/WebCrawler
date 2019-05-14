package webcrawler2.pkg0;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;

public class WebCrawlerForEmails {

    private final int NUM_OF_EMAILS = 10_000;
    private Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
    private Set<String> emails = Collections.synchronizedSet(new HashSet<>());
    private List<String> hyperlinks = Collections.synchronizedList(new LinkedList<>());
    private final String STARTING_URL = "https://www.touro.edu/";
    private final int NUM_OF_THREADS = 10;
    private HashSet<String> visitedLinks = new HashSet<>();
    private final int BATCH_SIZE = 25_000;

    public WebCrawlerForEmails() {
        hyperlinks.add(STARTING_URL);

    }

    public void scrape() {

        ExecutorService ex = Executors.newFixedThreadPool(NUM_OF_THREADS);
        do {
            if (!hyperlinks.isEmpty()) {
                if (!visitedLinks.contains(hyperlinks.get(0))) {
                    ex.execute(new multithreadedConnection(hyperlinks.get(0)));
                    visitedLinks.add(hyperlinks.get(0));
                    hyperlinks.remove(0);
                } else {
                    hyperlinks.remove(0);
                }
            }
        } while (emails.size() < NUM_OF_EMAILS);
        ex.shutdownNow();

        System.out.println(emails.size());
        System.out.println("links: " + visitedLinks.size());
        for (String s : emails) {
            System.out.println(s);
        }

        insertIntoDatabase();
    }

    private void insertIntoDatabase() {
        int count = 0;
        String connectionUrl
                = "jdbc:sqlserver://spring2019touro.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com;"
                + "database=databaseName;"
                + "user=userName;"
                + "password=password;"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        String insertSql = "INSERT INTO [WebCrawlers emails] ([Email Addresses]) VALUES "
                + "(?);";
        try (Connection connection = DriverManager.getConnection(connectionUrl);
                PreparedStatement statement = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);) {
            for(String s : emails){
                statement.setString(1, s);
                statement.addBatch();
                count++;
                if(count  == BATCH_SIZE ){
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
          
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class multithreadedConnection implements Runnable {

        String link;

        public multithreadedConnection(String link) {
            this.link = link;
       //     System.out.println(link);
        }

        @Override
        public void run() {
            try {
                // fetch the document over HTTP
                Document doc = Jsoup.connect(link).timeout(120_000).ignoreHttpErrors(true).
                        userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36").get();

                org.jsoup.select.Elements links = doc.select("a[href]");
                for (org.jsoup.nodes.Element link : links) {
                    // get the value from the href attribute
                    if (link.attr("abs:href").startsWith("mailto:") && link.attr("abs:href").contains("@") && emails.size() < NUM_OF_EMAILS && !link.attr("abs:href").contains("script")) {
                        String replace = link.attr("abs:href").replace("mailto:", "");
                        replace = replace.replace("%20", "");
                        if (replace.contains("?") || replace.contains("<") || replace.contains(",") || replace.contains(";") || replace.contains("&") || replace.contains("%")) {
                            Matcher matcher = p.matcher(replace);
                            while (matcher.find()) {
                                if (emails.size() < NUM_OF_EMAILS) {
                                    if (emails.add(matcher.group())) {
                                        System.out.println(emails.size());
                                    }
                                }
                            }
                        } else if (emails.add(replace)) {

                            System.out.println("email size: " + emails.size());
                        }

                    } else {
                        hyperlinks.add(link.attr("abs:href"));
                    }
                }

            } catch (UnsupportedMimeTypeException e) {
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        WebCrawlerForEmails wc = new WebCrawlerForEmails();
        wc.scrape();
    }

}
