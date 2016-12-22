package com.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.util.AppUtils;

public class MyCrawler extends Configured implements Tool {
	public static final Logger LOG = LoggerFactory.getLogger(MyCrawler.class);
	public static List<String> suffixList = new ArrayList<>();
	public static final String RESULT_PATH = "nutchdb/result";
	private int threads = 5;
	private int depth = 2;
	private long topN = 50;

	public MyCrawler(int threads, int depth, long topN) {

		this.threads = threads;
		this.depth = depth;
		this.topN = topN;
	}

	public MyCrawler() {
	}

	public List<String> getSuffixList() {
		return suffixList;
	}

	@Override
	public int run(String[] args) throws Exception {
		// input directory storing seed.txt
		Path rootUrlDir = new Path("urls");
		String suffix = AppUtils.getDate();
		suffixList.add(suffix);
		// output directory output file name
		Path dir = new Path(RESULT_PATH, "crawl-" + suffix);

		JobConf job = new NutchJob(getConf());

		FileSystem fs = FileSystem.get(job);

		Path crawlDb = new Path(dir + "/crawldb");
		Path segments = new Path(dir + "/segments");

		System.out.println("=============== Crawling ===============");
		Injector injector = new Injector(getConf());
		Generator generator = new Generator(getConf());

		// Fetcher fetcher = new Fetcher(getConf());
		// use customized fetch
		TextProcesser fetcher = new TextProcesser(getConf());
		ParseSegment parseSegment = new ParseSegment(getConf());
		CrawlDb crawlDbTool = new CrawlDb(getConf());

		// inject
		injector.inject(crawlDb, rootUrlDir);
		// generate for each depth
		int i;
		for (i = 0; i < depth; i++) {
			Path[] segs = generator.generate(crawlDb, segments, -1, topN, System.currentTimeMillis());
			if (segs == null) {
				LOG.info("No Urls at depth=" + i);
				break;
			}

			// fetch
			fetcher.fetch(segs[0], threads);

			if (!TextProcesser.isParsing(job)) {
				parseSegment.parse(segs[0]);
			}

			// update
			crawlDbTool.update(crawlDb, segs, true, true);

		}

		return 0;
	}

}
