package com.gun3y.nutch;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.scoring.webgraph.LinkRank;
import org.apache.nutch.scoring.webgraph.Loops;
import org.apache.nutch.scoring.webgraph.ScoreUpdater;
import org.apache.nutch.scoring.webgraph.WebGraph;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indexer extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(Indexer.class);

  public static void main(String args[]) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new Indexer(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 1) {
      System.out
          .println("Usage: Indexer [-dir crawlDir] [-solr url] [-indexOnly]");
      return -1;
    }
    Path crawlDir = null;
    String solrUrl = null;
    boolean indexOnly = false;

    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        crawlDir = new Path(args[i + 1]);
        i++;
      } else if ("-solr".equals(args[i])) {
        solrUrl = args[i + 1];
      } else if ("-indexOnly".equals(args[i])) {
        indexOnly = true;
      }
    }

    index(crawlDir, solrUrl, indexOnly);
    return 0;
  }

  private void index(Path crawlDir, String solrUrl, boolean indexOnly)
      throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Crawl = " + crawlDir);
      LOG.info("Solr Url = " + solrUrl);
    }

    Path crawlDb = new Path(crawlDir + "/crawldb");
    Path linkDb = new Path(crawlDir + "/linkdb");
    Path segments = new Path(crawlDir + "/segments");
    Path webgraphdb = new Path(crawlDir + "/webgraphdb");

    if (StringUtils.isBlank(solrUrl)) {
      LOG.warn("solrUrl is not set, indexing will be skipped...");
    } else {
      getConf().set("solr.server.url", solrUrl);
    }

    FileSystem fs = segments.getFileSystem(getConf());
    FileStatus[] fstats = fs.listStatus(segments,
        HadoopFSUtil.getPassDirectoriesFilter(fs));
    Path[] segPaths = HadoopFSUtil.getPaths(fstats);

    if (!indexOnly) {
      LinkDb linkDbTool = new LinkDb(getConf());
      linkDbTool.invert(linkDb, segPaths, true, true, false);
      linkDbTool.close();

      WebGraph webGraph = new WebGraph();
      webGraph.setConf(getConf());
      webGraph.createWebGraph(webgraphdb, segPaths, true, true);

      // Find the loops
      Loops loops = new Loops();
      loops.setConf(getConf());
      loops.findLoops(webgraphdb);

      // Calculate link ranks
      LinkRank linkRank = new LinkRank(getConf());
      linkRank.analyze(webgraphdb);

      // Update scores
      ScoreUpdater scoreUpdater = new ScoreUpdater();
      scoreUpdater.setConf(getConf());
      scoreUpdater.update(crawlDb, webgraphdb);
      scoreUpdater.close();
    }

    if (StringUtils.isNotBlank(solrUrl)) {
      IndexingJob indexingJob = new IndexingJob(getConf());
      indexingJob.index(crawlDb, linkDb, Arrays.asList(segPaths), false);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Index finished: " + crawlDir);
    }

  }
}
