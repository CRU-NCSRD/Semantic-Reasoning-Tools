/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.c2learn.tokenizers;

import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;
import com.aliasi.tokenizer.RegExFilteredTokenizerFactory;
import com.aliasi.tokenizer.Tokenizer;
import com.aliasi.tokenizer.TokenizerFactory;

public class NGramTokenizer {
	
	private boolean removeStopWords;
	private Set<String> stopWordSet;
	private int nGramOrder;
	
	public NGramTokenizer(boolean removeStopWords, Set<String> stopWordSet, int nGramOrder){
		this.removeStopWords = removeStopWords;
		this.stopWordSet = stopWordSet;
		this.nGramOrder = nGramOrder;
	}
		
		
	public Vector<String> tokenize(String input){
		
		Vector<String> nGrams = new Vector<String>();
		TokenizerFactory tf = IndoEuropeanTokenizerFactory.INSTANCE;
		Pattern pattern = Pattern.compile("[\\p{L}_]+|[\\!?}]+");
		RegExFilteredTokenizerFactory rtf = new RegExFilteredTokenizerFactory(tf,pattern);
		Tokenizer tokenizer;
		if (nGramOrder>1){
			   String[] prevs = new String[nGramOrder-1];
			   for (int i=0; i<prevs.length; i++){
				   prevs[i]="";
			   }
			   tokenizer = rtf.tokenizer(input.toCharArray(), 0, input.length());
				
			   String token = tokenizer.nextToken();
			   String ngram;
			   while(token!=null){
				   token = token.trim().toLowerCase();	
				   ngram = token;
                                   if(token.length()==1){
                                       
                                   }
				   if (removeStopWords && stopWordSet.contains(token)){
				   }
				   else {
					   
					   if (!prevs[prevs.length-1].equals("")){  ///find the ngram
						   for (int i=prevs.length-1; i>=0; i--){	
							   if (!prevs[i].equals(""))
	 						  ngram = prevs[i]+" "+ngram;		        					  
						   }
	 				   nGrams.add(ngram);
	                    
	                    for (int j=0; j<prevs.length-1; j++){
							   prevs[j] = prevs[j+1];
						   }
	                    prevs[prevs.length-1] = token;
					   }
					   else { //find the index to put the token
						   int tindex = prevs.length-1;
						   for (int i=prevs.length-2; i>=0; i--){
							   if (prevs[i].equals(""))
								   tindex=i;
						   }
						   prevs[tindex] = token;
						   
					   }
				   }
	            token = tokenizer.nextToken();
			   }
		
		   }
		   else {
			   tokenizer = rtf.tokenizer(input.toCharArray(), 0, input.length());
	
			   String token = tokenizer.nextToken();
			   while(token!=null){
				   token = token.trim().toLowerCase();
				   
				   if (removeStopWords && stopWordSet.contains(token)){
				   }
				   else {
					   nGrams.add(token);
				   }
	            token = tokenizer.nextToken();
			   }
		   }
		return nGrams;
	}
}

