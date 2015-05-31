package org.apache.nutch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crawler extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(Crawler.class);

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System
        .currentTimeMillis()));
  }

  /*
   * Perform complete crawling and indexing (to Solr) given a set of root urls
   * and the -solr parameter respectively. More information and Usage parameters
   * can be found below.
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new Crawler(), args);
    System.exit(res);
  }

  private void crawl(Path seedUrlsDir, Path crawlDir, int threads, int depth,
      long topN) throws IOException {
    JobConf job = new NutchJob(getConf());

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + crawlDir);
      LOG.info("rootUrlDir = " + seedUrlsDir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);
      if (topN != Long.MAX_VALUE)
        LOG.info("topN = " + topN);
    }

    Path crawlDb = new Path(crawlDir + "/crawldb");
    Path linkDb = new Path(crawlDir + "/linkdb");
    Path segments = new Path(crawlDir + "/segments");

    // Path tmpDir = job.getLocalPath("crawl" + Path.SEPARATOR + getDate());
    Injector injector = new Injector(getConf());
    Generator generator = new Generator(getConf());
    Fetcher fetcher = new Fetcher(getConf());
    ParseSegment parseSegment = new ParseSegment(getConf());
    CrawlDb crawlDbTool = new CrawlDb(getConf());
    LinkDb linkDbTool = new LinkDb(getConf());

    // initialize crawlDb
    injector.inject(crawlDb, seedUrlsDir);
    int i;
    for (i = 0; i < depth; i++) { // generate new segment
      Path[] segs = generator.generate(crawlDb, segments, -1, topN,
          System.currentTimeMillis());
      if (segs == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      fetcher.fetch(segs[0], threads); // fetch it
      if (!Fetcher.isParsing(job)) {
        parseSegment.parse(segs[0]); // parse it, if needed
      }
      crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
    }
    if (i > 0) {
      linkDbTool.invert(linkDb, segments, true, true, false); // invert
                                                              // links
    } else {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }

    CrawlDbReader crawlDbReader = new CrawlDbReader();
    crawlDbReader.setConf(getConf());
    JobConf readerJob = new NutchJob(getConf());
    crawlDbReader.processStatJob(crawlDb.toString(), readerJob, false);
    parseSegment.close();
    linkDbTool.close();
    crawlDbReader.close();

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl finished: " + crawlDir);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out
          .println("Usage: Crawler <urlDir> [-dir d] [-threads n] [-depth i] [-topN N]");
      return -1;
    }
    Path seedUrlsDir = null;
    Path crawlDir = new Path("crawl-" + getDate());
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    int depth = 5;
    long topN = Long.MAX_VALUE;

    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        crawlDir = new Path(args[i + 1]);
        i++;
      } else if ("-threads".equals(args[i])) {
        threads = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-depth".equals(args[i])) {
        depth = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-topN".equals(args[i])) {
        topN = Integer.parseInt(args[i + 1]);
        i++;
      } else if (args[i] != null) {
        seedUrlsDir = new Path(args[i]);
      }
    }

    crawl(seedUrlsDir, crawlDir, threads, depth, topN);
    return 0;
  }

}
