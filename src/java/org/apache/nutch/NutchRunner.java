package org.apache.nutch;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.scoring.webgraph.LinkRank;
import org.apache.nutch.scoring.webgraph.Loops;
import org.apache.nutch.scoring.webgraph.NodeDumper;
import org.apache.nutch.scoring.webgraph.ScoreUpdater;
import org.apache.nutch.scoring.webgraph.WebGraph;
import org.apache.nutch.util.NutchConfiguration;

public class NutchRunner {

  static String CRAWL = "crawl";
  static String CRAWL_DB = "crawl/crawldb";
  static String LINK_DB = "crawl/linkdb";
  static String WEBGRAPH_DB = "crawl/webgraphdb";
  static String SEGMENTS = "crawl/segments";
  static String SEED_URL = "crawl/urls";

  public static void main(String[] args) throws Exception {
    // stats();
    // invertLinks();
    // stats();
    // crawl2();
    indexer();
  }

  private static void indexer() throws Exception {
    Configuration conf = NutchConfiguration.create();
    ToolRunner.run(conf, new Indexer(), new String[] { "-dir",
        "/home/keysersoze/nutch/crawl" });
  }

  private static void solrindex() throws Exception {
    Configuration conf = NutchConfiguration.create();
    // .println("Usage: Indexer <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit] [-deleteGone] [-filter] [-normalize]");
    ToolRunner.run(conf, new IndexingJob(), new String[] { CRAWL_DB, "-linkdb",
        LINK_DB, "-dir", SEGMENTS, "-normalize" });
  }

  private static void score() throws Exception {
    Configuration conf = NutchConfiguration.create();

    ToolRunner.run(conf, new Loops(),
        new String[] { "-webgraphdb", WEBGRAPH_DB });
    ToolRunner.run(conf, new LinkRank(), new String[] { "-webgraphdb",
        WEBGRAPH_DB });
    ToolRunner.run(conf, new ScoreUpdater(), new String[] { "-crawldb",
        CRAWL_DB, "-webgraphdb", WEBGRAPH_DB });
    ToolRunner.run(conf, new NodeDumper(), new String[] { "-scores", "-topn",
        "1000", "-webgraphdb", WEBGRAPH_DB, "-output",
        WEBGRAPH_DB + "/dump/scores" });
  }

  private static void webgraph() throws Exception {
    Configuration conf = NutchConfiguration.create();

    WebGraph webGraph = new WebGraph();

    ToolRunner.run(conf, webGraph, new String[] { "-segmentDir", SEGMENTS,
        "-webgraphdb", WEBGRAPH_DB });
  }

  private static void stats() throws Exception {
    Configuration conf = NutchConfiguration.create();

    CrawlDbReader crawlDbReader = new CrawlDbReader();

    ToolRunner.run(conf, crawlDbReader, new String[] { CRAWL_DB, "-stats" });
  }

  private static void invertLinks() throws Exception {
    Configuration conf = NutchConfiguration.create();

    LinkDb linkDb = new LinkDb();

    ToolRunner.run(conf, linkDb, new String[] { LINK_DB, "-dir", SEGMENTS });
  }

  private static void crawl2() throws Exception {

    Configuration conf = NutchConfiguration.create();

    Crawler crawl = new Crawler();
    // Crawl <urlDir> -solr <solrURL> [-dir d] [-threads n] [-depth i]
    // [-topN N]
    ToolRunner.run(conf, crawl, new String[] { SEED_URL, "-dir", CRAWL + 2,
        "-threads", "5", "-depth", "2", "-topN", "10" });

  }

  private static void crawl() throws Exception {

    Configuration conf = NutchConfiguration.create();

    int numOfCrawl = 10;

    Injector injector = new Injector();
    Generator generator = new Generator();
    Fetcher fetcher = new Fetcher();
    ParseSegment parser = new ParseSegment();

    CrawlDb updateDb = new CrawlDb();

    ToolRunner.run(conf, injector, new String[] { CRAWL_DB, SEED_URL });

    for (int i = 0; i < numOfCrawl; i++) {
      System.out.println("NumOfCrawl:" + i);

      ToolRunner.run(conf, generator, new String[] { CRAWL_DB, SEGMENTS,
          "-topN", "1000" });

      List<String> segments = new ArrayList<String>();

      for (File file : (new File(SEGMENTS)).listFiles()) {
        segments.add(file.getAbsolutePath());
      }
      Collections.sort(segments, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return o2.compareTo(o1);
        }
      });

      String segmentDir = segments.get(0);

      ToolRunner.run(conf, fetcher, new String[] { segmentDir });

      ToolRunner.run(conf, parser, new String[] { segmentDir });

      ToolRunner.run(conf, updateDb, new String[] { CRAWL_DB, segmentDir });

    }
  }

}
