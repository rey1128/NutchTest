package de.xapio;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyCrawler extends Configured implements Tool {
	public static final Logger LOG = LoggerFactory.getLogger(MyCrawler.class);
	public static List<String> suffixList = new ArrayList<>();
	public static final String RESULT_PATH = "nutchdb/result";

	public List<String> getSuffixList() {
		return suffixList;
	}

	/**
	 * method to generate time suffix
	 * 
	 * @return
	 */
	private static String getDate() {
		return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
	}

	@Override
	public int run(String[] args) throws Exception {
		// input directory storing seed.txt
		Path rootUrlDir = new Path("urls");
		String suffix = getDate();
		suffixList.add(suffix);
		// output directory output file name
		Path dir = new Path(RESULT_PATH, "crawl-" + suffix);
		// number of threads
		int threads = 5;
		int depth = 2;
		long topN = 50;

		JobConf job = new NutchJob(getConf());

		FileSystem fs = FileSystem.get(job);

		// log info
		if (LOG.isInfoEnabled()) {
			LOG.info("crawl started in: " + dir);
			LOG.info("rootUrlDir = " + rootUrlDir);
			LOG.info("threads = " + threads);
			LOG.info("depth = " + depth);
			if (topN != Long.MAX_VALUE)
				LOG.info("topN = " + topN);
		}

		Path crawlDb = new Path(dir + "/crawldb");
		Path linkDb = new Path(dir + "/linkdb");
		Path segments = new Path(dir + "/segments");
		// Path indexes = new Path(dir + "/indexes");
		// Path index = new Path(dir + "/index");

		// Path tmpDir = job.getLocalPath("crawl" + Path.SEPARATOR + getDate());
		System.out.println("=============== Crawling ===============");
		Injector injector = new Injector(getConf());
		Generator generator = new Generator(getConf());

		// Fetcher fetcher = new Fetcher(getConf());
		// use customized fetch
		TextProcesser fetcher = new TextProcesser(getConf());
		ParseSegment parseSegment = new ParseSegment(getConf());
		CrawlDb crawlDbTool = new CrawlDb(getConf());
		LinkDb linkDbTool = new LinkDb(getConf());
		// DeduplicationJob deduplicationTool = new DeduplicationJob();
		// deduplicationTool.setConf(getConf());

		// inject
		injector.inject(crawlDb, rootUrlDir);
		int i;
		for (i = 0; i < depth; i++) { // generate new segment
			Path[] segs = generator.generate(crawlDb, segments, -1, topN, System.currentTimeMillis());
			if (segs == null) {
				LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
				break;
			}

			// fetch
			fetcher.fetch(segs[0], threads); // fetch it

			if (!TextProcesser.isParsing(job)) {
				parseSegment.parse(segs[0]); // parse it, if needed
			}

			// update
			crawlDbTool.update(crawlDb, segs, true, true); // update crawldb

		}

		// index into solr
		// get the segment paths
		FileStatus[] fstats = fs.listStatus(segments, HadoopFSUtil.getPassDirectoriesFilter(fs));
		Path[] paths = HadoopFSUtil.getPaths(fstats);


		// TODO remove duplicatation
		
		
		// System.out.println("=============== Indexing ===============");
		IndexingJob indexTool = new IndexingJob(getConf());

		// indexTool.index(crawlDb, linkDb, Arrays.asList(paths), false);

		// log info
		if (LOG.isInfoEnabled()) {
			LOG.info("crawl finished: " + dir);
		}

		return 0;
	}

}
