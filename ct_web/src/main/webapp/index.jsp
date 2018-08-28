<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8" %>
<%
    String path = request.getContextPath ();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Show Time</title>
</head>
<body>

<br/>
<form action="/queryCallLogList" method="post">
    姓名:<input type="text" name="name" value="">
    手机号码:<input type="text" name="telephone" value="">
    年:<input type="text" name="year" value="">
    月:<input type="text" name="month" value="">
    日:<input type="text" name="day" value="">
    <input type="submit" value="查询">
</form>
</body>
</html>
