<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.io.Text"
  import="org.apache.hadoop.hbase.HMaster"
    import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.HMaster.MetaRegion"
  import="org.apache.hadoop.hbase.HBaseAdmin"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.shell.ShowCommand"
  import="org.apache.hadoop.hbase.shell.TableFormatter"
    import="org.apache.hadoop.hbase.shell.ReturnMsg"
  import="org.apache.hadoop.hbase.shell.formatter.HtmlTableFormatter"
  import="org.apache.hadoop.hbase.HTableDescriptor" %><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  HBaseConfiguration conf = new HBaseConfiguration();
  TableFormatter formatter = new HtmlTableFormatter(out);
  ShowCommand show = new ShowCommand(out, formatter, "tables");
  HServerAddress rootLocation = master.getRootRegionLocation();
  Map<Text, MetaRegion> onlineRegions = master.getOnlineMetaRegions();
  Map<String, HServerInfo> serverToServerInfos =
    master.getServersToServerInfo();
%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>Hbase Master: <%= master.getMasterAddress()%></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>

<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="Hbase Logo" title="Hbase Logo" /></a>
<h1 id="page_title">Master: <%=master.getMasterAddress()%></h1>
<p id="links_menu"><a href="/hql.jsp">HQL</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a></p>
<hr id="head_rule" />

<h2>Master Attributes</h2>
<table>
<tr><th>Attribute Name</th><th>Value</th></tr>
<tr><td>Filesystem</td><td><%= conf.get("fs.default.name") %></td></tr>
<tr><td>Hbase Root Directory</td><td><%= master.getRootDir().toString() %></td></tr>
</table>

<h2>Online META Regions</h2>
<% if (rootLocation != null) { %>
<table>
<tr><th>Name</th><th>Server</th></tr>
<tr><td><%= HConstants.ROOT_TABLE_NAME.toString() %></td><td><%= rootLocation.toString() %></td></tr>
<%
  if (onlineRegions != null && onlineRegions.size() > 0) { %>
  <% for (Map.Entry<Text, HMaster.MetaRegion> e: onlineRegions.entrySet()) {
    MetaRegion meta = e.getValue();
  %>
  <tr><td><%= meta.getRegionName().toString() %></td><td><%= meta.getServer().toString() %></td></tr>
  <% }
  } %>
</table>
<% } %>

<h2>Tables</h2>
<% ReturnMsg msg = show.execute(conf); %>
<p><%=msg %></p>

<h2>Region Servers</h2>
<% if (serverToServerInfos != null && serverToServerInfos.size() > 0) { %>
<table>
<tr><th>Address</th><th>Start Code</th><th>Load</th></tr>
<%   for (Map.Entry<String, HServerInfo> e: serverToServerInfos.entrySet()) {
       HServerInfo hsi = e.getValue();
       String url = "http://" +
         hsi.getServerAddress().getBindAddress().toString() + ":" +
         hsi.getInfoPort() + "/";
       String load = hsi.getLoad().toString();
       long startCode = hsi.getStartCode();
       String address = hsi.getServerAddress().toString();
%>
<tr><td><a href="<%= url %>"><%= address %></a></td><td><%= startCode %></td><td><%= load %></tr>
<%   } %>
</table>
<% } %>
</body>
</html>
