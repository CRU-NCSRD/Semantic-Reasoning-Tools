package eu.c2learn.crawlers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

import java.util.ArrayList;
import net.billylieurance.azuresearch.AbstractAzureSearchQuery;

import net.billylieurance.azuresearch.AbstractAzureSearchResult;
import net.billylieurance.azuresearch.AzureSearchCompositeQuery;
import net.billylieurance.azuresearch.AzureSearchNewsResult;
import net.billylieurance.azuresearch.AzureSearchResultSet;
import net.billylieurance.azuresearch.AzureSearchWebResult;
import net.billylieurance.azuresearch.AbstractAzureSearchQuery.AZURESEARCH_QUERYTYPE;
import net.billylieurance.azuresearch.AzureSearchImageResult;
import net.billylieurance.azuresearch.AzureSearchRelatedSearchResult;

public class BingCrawler {

    private String appid;
    private String language;
    private Connection dbConnection;
    private long queryId;
    private int countResults = 0;

    public BingCrawler(String appid, Connection dbConnection) {
        this.appid = appid;
        this.dbConnection = dbConnection;
    }

    public BingCrawler(String appid, String language) {
        this.appid = appid;
        this.language = language;
    }

    private void setQueryId(long queryId) {
        this.queryId = queryId;
    }

    public int crawl(String query, long queryId) {
        try {
            setQueryId(queryId);
            String language = "en";
            language = NomadCrawlerUtils.identifyLanguage(query);
            if (language != null) {
                if (language.equals("en")) {
                    crawlWithMarket(query, "en-GB");
                    crawlWithMarket(query, "el-GR");
                    crawlWithMarket(query, "de-DE");
                } else if (language.equals("el")) {
                    crawlWithMarket(query, "el-GR");
                } else {
                    crawlWithMarket(query, "de-DE");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int countToReturn = countResults;
        countResults = 0;
        return countToReturn;
    }

    public ArrayList<String> crawl(String query) {
        ArrayList<String> results = new ArrayList<String>();
        try {
            if (this.language != null) {
                if (this.language.equals("en")) {
                    results.addAll(getURLs(query, "en-GB"));
                } else if (this.language.equals("el")) {
                    results.addAll(getURLs(query, "el-GR"));
                } else {
                    results.addAll(getURLs(query, "de-DE"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    public ArrayList<String> crawlImages(String query) {
        ArrayList<String> results = new ArrayList<String>();
        try {
            if (this.language != null) {
                if (this.language.equals("en")) {
                    results.addAll(getURLofImages(query, "en-GB"));
                } else if (this.language.equals("el")) {
                    results.addAll(getURLofImages(query, "el-GR"));
                } else {
                    results.addAll(getURLofImages(query, "de-DE"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    public ArrayList<String> crawlComposite(String query) {
        ArrayList<String> results = new ArrayList<String>();
        try {
            if (this.language != null) {
                if (this.language.equals("en")) {
                    results.addAll(getCompositeURLs(query, "en-GB"));
                } else if (this.language.equals("el")) {
                    results.addAll(getCompositeURLs(query, "el-GR"));
                } else {
                    results.addAll(getCompositeURLs(query, "de-DE"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    private void moveContent(long queryId) throws SQLException, ParseException {
        String sql = "SELECT id, url, clean_content, datetime, language "
                + "FROM bing_result "
                + "WHERE query_id = ?;";
        //try{
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = dbConnection.prepareStatement(sql);
            preparedStatement.setLong(1, queryId);
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();
            while (resultSet.next()) {
                String url = resultSet.getString("url");
                String clean_text = resultSet.getString("clean_content");
                String timestamp = resultSet.getString("datetime");
                String language = resultSet.getString("language");
                countResults += NomadCrawlerUtils.storeNomadContent(dbConnection, url, clean_text, timestamp, language, "bing_result", queryId);
            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private ArrayList<String> getURLs(String query, String market) {
        AzureSearchCompositeQuery compositeQuery = new AzureSearchCompositeQuery();

        AzureSearchCompositeQuery.adultToParam(AbstractAzureSearchQuery.AZURESEARCH_QUERYADULT.STRICT);
        compositeQuery.setAppid(appid);
        compositeQuery.setQuery(query);
        compositeQuery.setSources(new AZURESEARCH_QUERYTYPE[]{AZURESEARCH_QUERYTYPE.WEB});
        compositeQuery.setMarket(market);
        compositeQuery.setPerPage(10);//results to fetch
        compositeQuery.doQuery();
        ArrayList<String> results = new ArrayList<String>();

        AzureSearchResultSet<AbstractAzureSearchResult> ars = compositeQuery.getQueryResult();

        for (AbstractAzureSearchResult anr : ars) {
            try {
                String html;
                String title;
                String url;
                String urlHash;
                String date = null;
                String cleanedHtml = null;
                String cleanedHtmlHash;

                if (anr instanceof AzureSearchWebResult) {//fetch web results
                    AzureSearchWebResult web = (AzureSearchWebResult) anr;
                    title = web.getTitle();
                    url = web.getUrl();
                    results.add(url);
                    //countWeb++;
                } else {//fetch news results
                    AzureSearchNewsResult news = (AzureSearchNewsResult) anr;
                    title = news.getTitle();
                    url = news.getUrl();
                    results.add(url);
                    //countNews++;
                }

            } catch (NullPointerException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    private ArrayList<String> getCompositeURLs(String query, String market) {
        AzureSearchCompositeQuery compositeQuery = new AzureSearchCompositeQuery();
        AzureSearchCompositeQuery.adultToParam(AbstractAzureSearchQuery.AZURESEARCH_QUERYADULT.STRICT);
        compositeQuery.setAppid(appid);
        compositeQuery.setQuery(query);

        compositeQuery.setSources(new AZURESEARCH_QUERYTYPE[]{AZURESEARCH_QUERYTYPE.WEB, AZURESEARCH_QUERYTYPE.IMAGE});
        compositeQuery.setMarket(market);
        compositeQuery.setPerPage(50);//results to fetch

        compositeQuery.doQuery();
        ArrayList<String> results = new ArrayList<String>();

        AzureSearchResultSet<AbstractAzureSearchResult> ars = compositeQuery.getQueryResult();

        for (AbstractAzureSearchResult anr : ars) {
            try {
                String html;
                String title;
                String url;
                String urlHash;
                String date = null;
                String cleanedHtml = null;
                String cleanedHtmlHash;

                if (anr instanceof AzureSearchWebResult) {//fetch web results
                    AzureSearchWebResult web = (AzureSearchWebResult) anr;
                    title = web.getTitle();
                    url = web.getUrl();
                    results.add(url);
                } else {//fetch news results
                    AzureSearchImageResult news = (AzureSearchImageResult) anr;
                    title = news.getTitle();
                    url = news.getMediaUrl();
                    results.add(url);
                    //countNews++;
                }

            } catch (NullPointerException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    private ArrayList<String> getURLofImages(String query, String market) {
        //int countWeb = 0;
        //int countNews = 0;
        AzureSearchCompositeQuery.adultToParam(AbstractAzureSearchQuery.AZURESEARCH_QUERYADULT.STRICT);
        AzureSearchCompositeQuery compositeQuery = new AzureSearchCompositeQuery();
        compositeQuery.setAppid(appid);
        compositeQuery.setQuery(query);
        compositeQuery.setSources(new AZURESEARCH_QUERYTYPE[]{AZURESEARCH_QUERYTYPE.IMAGE});
        compositeQuery.setMarket(market);
        compositeQuery.setPerPage(50);//results to fetch
        compositeQuery.doQuery();
        ArrayList<String> results = new ArrayList<String>();

        AzureSearchResultSet<AbstractAzureSearchResult> ars = compositeQuery.getQueryResult();

        for (AbstractAzureSearchResult anr : ars) {
            try {
                String html;
                String title;
                String url;
                String urlHash;
                String date = null;
                String cleanedHtml = null;
                String cleanedHtmlHash;

                if (anr instanceof AzureSearchImageResult) {//fetch web results
                    AzureSearchImageResult web = (AzureSearchImageResult) anr;
                    title = web.getTitle();
                    url = web.getMediaUrl();
                    results.add(url);
                }

            } catch (NullPointerException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    private void crawlWithMarket(String query, String market) throws SQLException {
        
        
        AzureSearchCompositeQuery compositeQuery = new AzureSearchCompositeQuery();
        compositeQuery.setAppid(appid);
        compositeQuery.setQuery(query);
        compositeQuery.setSources(new AZURESEARCH_QUERYTYPE[]{AZURESEARCH_QUERYTYPE.WEB, AZURESEARCH_QUERYTYPE.NEWS});
        compositeQuery.setMarket(market);
        compositeQuery.setPerPage(50);//results to fetch
        compositeQuery.doQuery();
        

        AzureSearchResultSet<AbstractAzureSearchResult> ars = compositeQuery.getQueryResult();

        for (AbstractAzureSearchResult anr : ars) {
            try {
                String html;
                String title;
                String url;
                String urlHash;
                String date = null;
                String cleanedHtml = null;
                String cleanedHtmlHash;

                if (anr instanceof AzureSearchWebResult) {//fetch web results
                    AzureSearchWebResult web = (AzureSearchWebResult) anr;
                    title = web.getTitle();
                    url = web.getUrl();
                    //countWeb++;
                } else {//fetch news results
                    AzureSearchNewsResult news = (AzureSearchNewsResult) anr;
                    title = news.getTitle();
                    url = news.getUrl();
                    date = news.getDate();
                    date = date.replace("T", " ");
                    date = date.replace("Z", "");
                    //countNews++;
                }

                urlHash = DigestUtils.md5Hex(url);
                html = getHtml(url);

                cleanedHtml = ArticleExtractor.INSTANCE.getText(html);//clean with boilerpipe

                if (!(cleanedHtml == null || cleanedHtml.equals(""))) {//if the retrieved content is empty, do nothing

                    cleanedHtmlHash = DigestUtils.md5Hex(cleanedHtml);

                    Long id = retrieveMostRecentID(urlHash, queryId);

                    if (id == null || id == 0) {
                        insertResult(query, url, urlHash, title, html, cleanedHtml, cleanedHtmlHash, market, date);
                    } else {

                        if (date == null) {
                            if (!contentHashIsEqual(id, cleanedHtmlHash)) {
                                insertResult(query, url, urlHash, title, html, cleanedHtml, cleanedHtmlHash, market, date);
                            }
                        } else {
                            Date dateFormated = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
                            Date dbDate = retrieveLatestDateFromDB(id);
                            if (dateFormated.after(dbDate)) {
                                insertResult(query, url, urlHash, title, html, cleanedHtml, cleanedHtmlHash, market, date);
                            }
                        }

                    }

                }

            } catch (ClientProtocolException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (BoilerpipeProcessingException e) {
                e.printStackTrace();
            } catch (NullPointerException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        
    }

    public String getHtml(String url) throws IOException {
        HttpGet request = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        StringBuilder html = new StringBuilder("");
        try {
            HttpClient client = new DefaultHttpClient();
            request = new HttpGet(url);
            HttpResponse response = client.execute(request);
        
            inputStreamReader = new InputStreamReader(response.getEntity().getContent());
            bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                html.append(line);
            }
        
        
        
            return html.toString();
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
        }
    }

    private boolean contentHashIsEqual(long id, String cleanedHtmlHash) throws SQLException {
        boolean isEqual = false;
        String sql = "SELECT clean_content_hash FROM bing_result WHERE id = ?";
        //try {
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setLong(1, id);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        while (resultSet.next()) {
            String dbHash = resultSet.getString("clean_content_hash");
            if (dbHash.equals(cleanedHtmlHash)) {
                isEqual = true;
            }
        }
        preparedStatement.close();
        return isEqual;
    }

    private Long retrieveMostRecentID(String urlHash, long query_id) throws SQLException {
        Long id = null;
        String sql = "SELECT id FROM bing_result WHERE url_hash LIKE ? AND query_id = ? AND datetime = (SELECT MAX(datetime) FROM bing_result WHERE url_hash LIKE ? AND query_id = ?);";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, urlHash);
        preparedStatement.setLong(2, query_id);
        preparedStatement.setString(3, urlHash);
        preparedStatement.setLong(4, query_id);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        if (resultSet.next()) {
            id = resultSet.getLong("id");
        }
        preparedStatement.close();
        return id;
    }

    private Date retrieveLatestDateFromDB(Long id) throws SQLException {
        String sql = "SELECT datetime FROM bing_result WHERE id = ?;";
        Date date = null;
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setLong(1, id);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        if (resultSet.next()) {
            date = resultSet.getTimestamp("datetime");
        }
        preparedStatement.close();
        return date;
    }

    private String retrievedDateFromUrl(String cleanedHtml) {
        return null;
    }

    private void insertResult(String query, String url, String urlHash, String title, String html, String cleanedHtml, String cleanedHtmlHash, String market, String date) throws SQLException {
        String sql = "INSERT INTO bing_result(query, url, url_hash, title, html_content, clean_content, clean_content_hash, market, language, datetime, query_id)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = dbConnection.prepareStatement(sql);
            preparedStatement.setString(1, query);
            preparedStatement.setString(2, url);
            preparedStatement.setString(3, urlHash);
            preparedStatement.setString(4, title);
            preparedStatement.setBinaryStream(5, IOUtils.toInputStream(html));
            preparedStatement.setString(6, cleanedHtml);//TODO: java.sql.SQLException: Incorrect string value: '\xF3\x93\xB1\x94\xC9\xB7...' for column 'clean_content' at row 1
            preparedStatement.setString(7, cleanedHtmlHash);
            preparedStatement.setString(8, market);
            preparedStatement.setString(9, NomadCrawlerUtils.identifyLanguage(cleanedHtml));//null if language could not identify
            if (date == null) {//if date is null try to retrieve it from the page
                date = retrievedDateFromUrl(cleanedHtml);
            }
            if (date == null) {//if date could not be retrieved insert current date
                Calendar cal = Calendar.getInstance();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                preparedStatement.setString(10, sdf.format(cal.getTime()));
            } else {
                preparedStatement.setString(10, date);
            }
            preparedStatement.setLong(11, queryId);
            countResults += preparedStatement.executeUpdate();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }
}
