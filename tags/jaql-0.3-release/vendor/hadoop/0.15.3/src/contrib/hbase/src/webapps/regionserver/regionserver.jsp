<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.io.Text"
  import="org.apache.hadoop.hbase.HRegionServer"
  import="org.apache.hadoop.hbase.HRegion"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HRegionInfo" %><%
  HRegionServer regionServer = (HRegionServer)getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  HServerInfo serverInfo = regionServer.getServerInfo();
  SortedMap<Text, HRegion> onlineRegions = regionServer.getOnlineRegions();
%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>Hbase Region Server: <%= serverInfo.getServerAddress().toString() %></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="Hbase Logo" title="Hbase Logo" /></a>
<h1 id="page_title">Region Server: <%= serverInfo.getServerAddress().toString() %></h1>
<p id="links_menu"><a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a></p>
<hr id="head_rule" />

<h2>Region Server Attributes</h2>
<table>
<tr><th>Attribute Name</th><th>Value</th></tr>
<tr><td>Load</td><td><%= serverInfo.getLoad().toString() %></td></tr>
</table>

<h2>Online Regions</h2>
<% if (onlineRegions != null && onlineRegions.size() > 0) { %>
<table>
<tr><th>Region Name</th><th>Start Key</th><th>End Key</th></tr>
<%   for (HRegion r: onlineRegions.values()) { %>
<tr><td><%= r.getRegionName().toString() %></td><td><%= r.getStartKey().toString() %></td><td><%= r.getEndKey().toString() %></td></tr>
<%   } %>
</table>
<p>Region names are made of the containing table's name, a comma,
the start key, a comma, and a randomly generated region id.  To illustrate,
the region named
<em>domains,apache.org,5464829424211263407</em> is party to the table 
<em>domains</em>, has an id of <em>5464829424211263407</em> and the first key
in the region is <em>apache.org</em>.  The <em>-ROOT-</em>
and <em>.META.</em> 'tables' are internal sytem tables.
The -ROOT- keeps a list of all regions in the .META. table.  The .META. table
keeps a list of all regions in the system. The empty key is used to denote
table start and table end.  A region with an
empty start key is the first region in a table.  If region has both an empty
start and an empty end key, its the only region in the table.  See
<a href="http://wiki.apache.org/lucene-hadoop/Hbase">Hbase Home</a> for
further explication.<p>
<% } else { %>
<p>Not serving regions</p>
<% } %>
</body>
</html>
