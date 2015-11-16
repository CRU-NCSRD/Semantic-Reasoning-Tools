/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.c2learn.tokenizers;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 *
 * @author admin
 */
public class HTMLPages implements Runnable {

    String url;
    Thread runner;
    Set<String> pages;
    String language;

    
    public HTMLPages(String url, Set<String> pages, String language) {
        this.url = url;
        this.language = language;
        runner = new Thread(this, this.url);
        this.pages = pages;
        runner.start();

    }

    public void run() {
        try {
            getPagesList(pages);
        } catch (IOException ex) {
            Logger.getLogger(HTMLPages.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BoilerpipeProcessingException ex) {
            Logger.getLogger(HTMLPages.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void getPagesList(Set<String> pages) throws IOException, BoilerpipeProcessingException {//ConcurrentHashMap<String, Double> tokensHashMap, ConcurrentHashMap<String, Double> sentenceHashMap) {

       
            String html = getHtml(url);
            String cleanHtml = ArticleExtractor.INSTANCE.getText(html);
            if (!cleanHtml.equalsIgnoreCase("")) {
                pages.add(cleanHtml);
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
            if (Thread.interrupted()) {
                return "";
            }
            inputStreamReader = new InputStreamReader(response.getEntity().getContent(), "UTF8");
            bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                html.append(line);
            }
            return html.toString();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return "";
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

}
