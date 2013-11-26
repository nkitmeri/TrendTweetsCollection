/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package testpackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 *
 * @author nikos
 */
public class TestClass {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException 
    {
        BufferedReader br = new BufferedReader( new InputStreamReader(
            new FileInputStream( new File( "/tmp/list.txt" ) ) ) );
        String line;
        int cnt = 0;
        
        while( ( line = br.readLine() ) != null )
        {
            if( cnt++ % 10 != 0 && line.contains( " " ) )
            {
                TokenStream tokenizer = new StandardTokenizer(
                        Version.LUCENE_46, new StringReader( line ) );
                
                tokenizer = new ShingleFilter( tokenizer, 2, 5 );
                CharTermAttribute termAtt = tokenizer
                        .addAttribute( CharTermAttribute.class );
                tokenizer.reset();
                while( tokenizer.incrementToken() )
                {
                    if( termAtt.toString().split( "\\s+" ).length >= 6 )
                        System.out.println( termAtt.toString() );
                }
                
            }
        }
        
    }
    
}
