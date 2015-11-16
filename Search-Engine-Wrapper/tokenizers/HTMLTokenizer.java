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
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
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
public class HTMLTokenizer implements Runnable{
    String url;
    Set<String> stopWordSet;
    Thread runner;
    ConcurrentHashMap<String, double[]> tokensHashMap;
    ConcurrentHashMap<String, Double> sentenceHashMap;

    public HTMLTokenizer(String url, Set<String> stopWordSet,ConcurrentHashMap<String, double[]> tokensHashMap,ConcurrentHashMap<String, Double> sentenceHashMap) {
        this.url = url;
        this.stopWordSet = stopWordSet;
        runner=new Thread(this,this.url);        
        this.tokensHashMap=tokensHashMap;
        this.sentenceHashMap=sentenceHashMap;           
        runner.start();
    }    
    
    public void run(){        
        getTokensList(tokensHashMap,sentenceHashMap);
        
    }
    
    public void getTokensList(ConcurrentHashMap<String, double[]> tokensHashMap,ConcurrentHashMap<String, Double> sentenceHashMap){                      
        
        try {                
                String html=getHtml(url);     
                String cleanHtml=ArticleExtractor.INSTANCE.getText(html);
                System.out.println(cleanHtml);
                int nGramOrder=1;
		
                NGramTokenizer tokenizer = new NGramTokenizer(true, stopWordSet, nGramOrder);
                Vector<String> tokenList=tokenizer.tokenize(cleanHtml);        
                
                String token;
                String ngram;
                for (int t=0; t<tokenList.size(); t++){
                    token = tokenList.get(t);
                    token = token.trim().toLowerCase();
                    if (token.length() < 4){
                        continue;
                    }
                    ngram = token;
                    
                    sentenceHashMap.putIfAbsent(ngram, new Double(0.0));       
                    
                    Double sentenceTokenCount = (Double) sentenceHashMap.get(ngram);
                    double temp = sentenceTokenCount.doubleValue() + 1.0;
                    sentenceHashMap.put(ngram, new Double(temp));
                }
                
                Iterator<String> it = sentenceHashMap.keySet().iterator();
                String key;
                while (it.hasNext()){
                    key = (String)it.next();                    
                    Double stc = (Double) sentenceHashMap.get(key); 
                    double tc, sc;
                    double[] newTokenValues = new double[2];
                    
                    newTokenValues[0] = stc.doubleValue();
                    newTokenValues[1] = 1.0;
                    
                    double[] check=tokensHashMap.putIfAbsent(key, newTokenValues);
                    
                    if(check!=null){                        
                        double[] tokenValues = tokensHashMap.get(key);
                        tc = tokenValues[0]+stc.doubleValue();
                        sc = tokenValues[1]+1.0;
                    
                        newTokenValues[0] = tc;
                        newTokenValues[1] = sc;
                    
                        tokensHashMap.put(key, newTokenValues);
                        
                    }
                    
                }
                sentenceHashMap.clear();
                
            } catch (IOException ex) {
                Logger.getLogger(HTMLPages.class.getName()).log(Level.SEVERE, null, ex);
            } catch (BoilerpipeProcessingException ex) {
                Logger.getLogger(HTMLPages.class.getName()).log(Level.SEVERE, null, ex);
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
			// Get the responserequest.releaseConnection();
			inputStreamReader = new InputStreamReader(response.getEntity().getContent(),"UTF8");
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
        
}
    

