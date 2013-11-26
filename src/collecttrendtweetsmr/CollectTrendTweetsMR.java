package collecttrendtweetsmr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author nikos
 */
public class CollectTrendTweetsMR extends Configured implements Tool
{
    public static class TrendsMapper extends Mapper< LongWritable, Text,
            Text, Text >
    {
        private static HashSet< String > trendSet;
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        
        @Override
        public void setup( Context context )
        {
            FSDataInputStream in = null;
            BufferedReader br = null;
            int cnt = 0;
            
            try
            {
                FileSystem fs = FileSystem.get( context.getConfiguration() );
                Path[] paths = DistributedCache.getLocalCacheFiles(
                    context.getConfiguration() );
                in = fs.open( paths[0] );
                br = new BufferedReader( new InputStreamReader( in ) );  
                String line;
                trendSet = new HashSet<>();
                
                while( ( line = br.readLine() ) != null )
                {
                    if( cnt++ % 10 != 0 )
                        trendSet.add( line );//.split( "," )[0] );
                }
                
                in.close();
            }
            catch( Exception e )
            {
                e.printStackTrace( System.out );
            }
        }
        
        @Override
        public void map( LongWritable key, Text value, Context context )
        {
            try
            {
                Status tweet = DataObjectFactory
                        .createStatus( value.toString() );
                TokenStream tokenizer = new StandardTokenizer(
                    Version.LUCENE_46, new StringReader( value.toString() ) );
                tokenizer = new ShingleFilter( tokenizer, 2, 5 );

                CharTermAttribute termAtt = tokenizer
                        .addAttribute( CharTermAttribute.class );
                tokenizer.reset();
                
                String token;
                
                while( tokenizer.incrementToken() )
                {
                    token = termAtt.toString();
                    
                    if( trendSet.contains( token ) )
                    {   
                        outKey.set( token );
                        outValue.set( tweet.getId() + ","
                                + tweet.getCreatedAt() );
                        
                        context.write( outKey, outValue );
                    }
                }
            }
            catch( Exception e)
            {
                e.printStackTrace( System.out );
            }
        }
    }
    
    public static class TrendsReducer extends Reducer< Text, Text, Text, Text >
    {
        @Override
        public void reduce( Text key, Iterable< Text > values, Context context )
                throws IOException, InterruptedException
        {
            for( Text value : values )
            {
                context.write( key, value );
            }
        }
    }

    @Override
    public int run( String[] args ) throws IOException, URISyntaxException,
            InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();
        DistributedCache.addCacheFile( new URI( "/tmp/list.txt" ), conf );
        Job job1 = new Job( conf );
        
        job1.setJobName( "trends_collection" );
        job1.setJarByClass( getClass() );
        job1.setMapperClass( TrendsMapper.class );
        job1.setReducerClass( TrendsReducer.class );
        job1.setOutputKeyClass( Text.class );
        job1.setOutputValueClass( Text.class );
        
        FileInputFormat.addInputPaths( job1, args[0] );
        FileOutputFormat.setOutputPath( job1, new Path( args[1] ) );
        
        
        return job1.waitForCompletion( true ) ? 0 : 1;
    }
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser( conf, args ).
                getRemainingArgs();
        int exit = ToolRunner.run( conf, new CollectTrendTweetsMR(),
                otherArgs );
        
        System.exit( exit );
    }
    
}
