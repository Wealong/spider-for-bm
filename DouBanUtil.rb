# ===================================================================
#     FileName: DouBanUtil.rb
#       Author: Wealong
#     Function: 爬取豆瓣图书或者电影资料
#        Email: wealong0@gmail.com
#   CreateTime: 2014-08-12 17:42
# ===================================================================
# encoding: utf-8
#!/usr/bin/env
require "open-uri"
require 'nokogiri'
require 'useragents'
require 'thread'
require 'sqlite3'
require 'set'

# goagent的代理
$proxys = [["http://127.0.0.1:8087","",""]]
$MAX_THREAD = 10              # => 最大线程数
$MAX_PAGES = 100              # => 每个标签最大搜索页面
$MIN_COMMENT_TIMES = 10       # => 至少评价次数
$MIN_COMMENT_LEVEL = 0      # => 至少评价等级
$Count = 0                    # => 计数
$set = Set::new

# 用正则表达式解析图书页面
class Book
  attr_accessor :url,:title,:author,:ISBN,:summary,:comment_time,:comment_level
  @url = "", @title = "", @author = "", @ISBN = "", @summary = "", @comment_time = 0, @comment_level = 0.0
  def initialize(url, html)
    @url = url
    @title = getMessageByRegexp(%r{<span\s*property="v:itemreviewed">\s*(.+)\s*</span\s*>},html)
    @author = getAuthor(html)
    @ISBN = getMessageByRegexp(%r{ISBN:\s*</span>\D*(\d+)\D*<br/>},html)
    @summary = getMessageByRegexp(%r{<div\s*class="intro">\s*(.*?)\s*</div>},html)
    @comment_time = getMessageByRegexp(%r{<span\s*property="v:votes">\D*(\d+)\D*</span>},html)
    @comment_level = getMessageByRegexp(%r{<strong\s*class="ll rating_num "\s*property="v:average">\s*(\d\.\d)\s*</strong>},html)
  end
  def getMessageByRegexp(regexp,html)
    match = regexp.match(html)
    (match)?match[1]:""
  end
  def getAuthor(html)
    temp = getMessageByRegexp(%r{</span>:\s*(.+?)\s*</span\s*>}m,html)
    mans = ""
    temp.scan(%r{>(.+?)<}) do |man|
      mans = mans + "/"  + man[0].to_s
    end
    mans[1..-1]?mans[1..-1]:""
  end
  def to_s
    "============================================\n" +
      " 标题:"+ @title + "\n" +
      " 作者:"+ @author + "\n" +
      " ISBN:"+ @ISBN + "\n" +
      " 链接:"+ @url + "\n" +
      " " + @comment_time.to_s + "人评价\n" +
      " 综合评价:" + @comment_level.to_s + "星\n"  +
      " 简介:"+ Nokogiri::HTML(@summary) + "\n" +
      "============================================\n"
  end
end

# 用Nokogiri解析电影界面
class Movie
  attr_accessor :url,:title,:director,:writer,:actor,:summary,:comment_time,:comment_level
  @url = "", @title = "", @director = "",@writer = "", @actor = "", @summary = "", @comment_time = 0, @comment_level = 0.0
  def initialize(url,html)
    @url = url
    page = Nokogiri::HTML(html)
    @title = page.css("h1 span").text
    @director =  page.css("#info a[rel='v:directedBy']").text
    @actor = ""
    page.css("#info a[rel='v:starring']").each do |a|
      @actor += "," + a
    end
    @actor = @actor[1..-1] unless @actor.empty?
    @writer = ""
    page.css("#info span")[2].css("a").each do |a|
      @writer += "," + a
    end
    @writer = @writer[1..-1] unless @writer.empty?
    @summary = page.css("span.hidden").text
    if @summary.empty?
      @summary = page.css("span[property='v:summary']").text
    end
    @summary = @summary.gsub(" ","").gsub("\n\n","\n")
    @comment_time = page.css("span[property='v:votes']").text.to_i
    @comment_level = page.css("strong[property='v:average']").text.to_f
  end
  def to_s
    "===========================================================\n" +
      " 标题:"+ @title + "\n" +
      " 导演:"+ @director + "\n" +
      " 编剧:"+ @writer + "\n" +
      " 主演:"+ @actor + "\n" +
      " 链接:"+ @url + "\n" +
      " " + @comment_time.to_s + "人评价\n" +
      " 综合评价:" + @comment_level.to_s + "星\n"  +
      " 简介:"+ Nokogiri::HTML(@summary) + "\n" +
      "===========================================================\n"

  end
end

class DataBaseUtil
  attr_accessor :subject,:db
  def initialize subject
    @subject = subject
    @db = SQLite3::Database.new @subject + ".db"
  end

  def create clazz
    if table_exist?(clazz) then return end
    vars = clazz.instance_variables
    createSQL =  " CREATE TABLE %s( _id integer primary key," % clazz.name
    vars.each do |var|
      createSQL += ("_%s ," % var[1..-1])
    end
    createSQL = createSQL[0..-2]
    createSQL += ")"
    @db.execute createSQL
  end

  def table_exist? clazz
    sql = "SELECT * FROM MAIN.[sqlite_master] WHERE tbl_name = ?"
    return !@db.execute(sql, [clazz.name]).empty?
  end

  def insert object
    # return if item_exist? object
    vars = object.instance_variables
    insertSQL = "INSERT INTO %s(" % object.class
    parms = []
    vars.each do |var|
      insertSQL += ("_%s ," % var[1..-1])
      parms << object.instance_variable_get(var)
    end
    insertSQL = insertSQL[0..-2] + ") VALUES(?" + ",?" * (vars.count - 1) + ")"
    @db.execute insertSQL,parms
  end

  def item_exist? object
    sql = "SELECT _id FROM %s WHERE _title = ?" % object.class
    return !@db.execute(sql,[object.title]).empty?
  end
end
class DouBanUtil

  attr_accessor :subject,:db

  def initialize(subject)
    @subject = subject
    @agent = UserAgents.rand()
    @proxy = $proxys[rand($proxys.size)]
    @db = DataBaseUtil.new @subject
  end

  def search_book(keyword)
    @file = File.new("搜索" + keyword + "关键字得到的书单.txt","w:utf-8")
    key = URI.escape(keyword)
    $MAX_PAGES.times do |i|
      puts "正在搜索第" + (i+1).to_s + "页..."
      book_analyze(searchurl(key,i))
    end
    @file.close
  end

  def tag_book(keyword)
    @file = File.new(keyword + ".txt","w:utf-8")
    key = URI.escape(keyword)
    $MAX_PAGES.times do |i|
      puts Thread.current.to_s + "正在搜索第" + (i+1).to_s + "页..."
      book_analyze(tagurl(key,i))
    end
    @file.close
  end
  def tag_movie(keyword)
    key = URI.escape(keyword)
    $MAX_PAGES.times do |i|
      puts Thread.current.to_s + "正在搜索第" + (i+1).to_s + "页..."
      movie_analyze(tagurl(key,i))
    end
  end

  def book_analyze(url)
    html = openbyproxy(url).read.force_encoding("ISO-8859-1").encode("utf-8", replace: nil)

    html.scan(%r{<li\s+class\s*=\s*"subject-item"\s*>(.+?)</li\s*>}m) do |book|
      times = comment_time(book[0]).to_i
      level = comment_level(book[0]).to_f
      if times>$MIN_COMMENT_TIMES && level>$MIN_COMMENT_LEVEL
        book_url = book_url(book[0])
        book = Book.new(book_url,openbyproxy(book_url).read)
        @db.insert book
        $Count += 1
        p $Count.to_s + book.title
        sleep 0.1
        @file.puts(book)
      end
    end
  end

  def movie_analyze(url)
    html = openbyproxy(url).read.force_encoding("ISO-8859-1").encode("utf-8", replace: nil)

    html.scan(%r{<tr\s+class\s*=\s*"item"\s*>(.+?)</tr\s*>}m) do |item|
      title = getTitle(item[0])
      if $set.include? title
        next
      else
        $set << title
      end

      times = comment_time(item[0]).to_i
      level = comment_level(item[0]).to_f
      if times>$MIN_COMMENT_TIMES && level>$MIN_COMMENT_LEVEL
        movie_url = movie_url(item[0])
        movie = Movie.new(movie_url,openbyproxy(movie_url).read)
        $Count += 1
        p $Count.to_s + movie.title
        sleep 0.1
        @db.insert movie
      end
    end
  end


  def searchurl(key,i)
    if i == 0
      "http://%s.douban.com/subject_search?search_text=%s" % [@subject,key]
    elsif i > 0
      "http://%s.douban.com/subject_search?start=%d&search_text=%s" % [@subject,i*15,key]
    end
  end

  def tagurl(key,i)
    if i == 0
      "http://%s.douban.com/tag/%s" % [@subject,key]
    elsif i > 0
      "http://%s.douban.com/tag/%s?start=%d" % [@subject,key,i*20]
    end
  end

  def getTitle(item)
    re = %r{<a\s*class="nbg".*title="(.*)">}
    match = re.match(item)
    (match)?match[1]:""
  end

  def comment_time(item)
    re = %r{<span\s*class\s*=\s*"pl">\s*\D*(\d+)\D*\s*</span\s*>}
    match = re.match(item)
    (match)?match[1]:0
  end
  def comment_level(item)
    re = %r{<span\s*class\s*=\s*"rating_nums"\s*>\s*(\d\.\d)\s*</span\s*>}
    match = re.match(item)
    (match)?match[1]:0
  end
  def book_url(item)
    re = %r{<a\s*href=\s*"(.+)"\s*title=}
    match = re.match(item)
    (match)?match[1]:""
  end
  def movie_url(item)
    re = %r{<a\s*class="nbg"\s*href=\s*"(.+)"\s*title=}
    match = re.match(item)
    (match)?match[1]:""
  end
  def alltags
    page = Nokogiri::HTML(openbyproxy("http://%s.douban.com/tag/" % @subject))
    tags = []
    page.css("table.tagCol").each do |table|
      table.css("td a").each do |td|
        tags << td.text
      end
    end
    tags
  end
  def openbyproxy url
    n = 0
    begin
      html = open(url,"User-Agent" => @agent,:proxy_http_basic_authentication => @proxy)
    rescue Exception => ex
      @agent = UserAgents.rand()
      @proxy = $proxys[rand($proxys.size)]
      p @proxy
      n += 1
      retry if n<6
      p ex
      p @proxy + "已挂勿念"
      Thread.exit
    end
    html
  end
  def self.findBook
    myUtil = DouBanUtil.new("book")

    tags = myUtil.alltags
    threads = []
    while !tags.empty?
      if threads.size < $MAX_THREAD
        threads << Thread.new do
          begin
            tag = tags.shift
            DouBanUtil.new("book").tag_book(tag)
          rescue
            p "错误跳过重来"
            retry
          end
        end
      else
        threads.each { |t| t.join}
        threads.clear
      end
    end
  end
  def self.findMovie
    myUtil = DouBanUtil.new("movie")
    DataBaseUtil.new("movie").create Movie
    tags = myUtil.alltags
    threads = []
    while !tags.empty?
      if threads.size < $MAX_THREAD
        threads << Thread.new do
          tag = tags.shift
          begin
            DouBanUtil.new("movie").tag_movie(tag)
          rescue
            p "错误重来"
            retry if tag != ""
          end
        end
      else
        threads.each { |t| t.join}
        threads.clear
      end
    end
  end
end

# DouBanUtil.new("book").search_book "ruby"
# DataBaseUtil.new("DouBan").create Book
  DouBanUtil.findMovie
# DouBanUtil.findBook

