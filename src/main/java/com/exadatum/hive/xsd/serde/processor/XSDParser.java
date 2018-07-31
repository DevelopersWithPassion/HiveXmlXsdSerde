package com.exadatum.hive.xsd.serde.processor;

import com.exadatum.hive.xsd.serde.readerwriter.XmlInputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class XSDParser {
    private static String rootElement = "";

    public static Map<String, String> getAllXpaths(File file) {
        Map<String, String> result = new HashMap<String, String>();
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document document = docBuilder.parse(file);
            NodeList elementList = document.getElementsByTagName("xs:element");
            NodeList attributeList = document.getElementsByTagName("xs:attribute");
            for (int i = 0; i < elementList.getLength(); i++) {
                Element element = (Element) elementList.item(i);
                if (element.hasAttributes() && element.getAttribute("type") != "") {
                    result.put(xpathString(element, new StringBuilder(element.getAttribute("name"))).toString(), extractType(element.getAttribute("type")));
                }
                if (i <= 0) {
                    setStartAndEndTag(element);
                }
            }
            for (int i = 0; i < attributeList.getLength(); i++) {
                Element element = (Element) attributeList.item(i);
                if (element.hasAttributes() && element.getAttribute("type") != "") {
                    result.put(element.getAttribute("name"), extractType(element.getAttribute("type")));
                }

            }

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException ed) {
            ed.printStackTrace();
        }
        return getValidXpath(result);
    }


    public static void main(String args[]) {
        Map<String, String> xpathWithType = getAllXpaths(new File("/home/exa00077/test.xsd"));
       /* ;
        for (Map.Entry<String, String> entry : xpathWithType.entrySet()) {
            System.out.println(entry.getKey() + " -->  " + entry.getValue());
        }
*/

        for (Map.Entry<String, String> entry : xpathWithType.entrySet()) {
            System.out.println(entry.getKey() + " -->  " + entry.getValue());
        }

        System.out.println(XmlInputFormat.startTag + " ENd tag" + XmlInputFormat.endTag);
    }


    private static StringBuilder xpathString(Element element, StringBuilder xpath) {
        Element parent = getParentElement(element);
        if (parent != null) {
            if (parent.hasAttributes() && parent.getAttribute("name") != "") {
                xpath = new StringBuilder(parent.getAttribute("name")).append("/").append(xpath);
                return xpathString(parent, xpath);
            } else {
                return xpath;
            }
        } else {
            return xpath;
        }
    }

    private static Element getParentElement(Element element) {
        Node parentNode = element.getParentNode();
        if (parentNode != null && parentNode instanceof Element) {
            String nodeName = parentNode.getNodeName();
            if (nodeName.equalsIgnoreCase("xs:element")) {
                return (Element) element.getParentNode();
            } else {
                return getParentElement((Element) parentNode);
            }
        } else {
            return null;
        }
    }

    private static String extractType(String type) {
        if (type.startsWith("xs:")) {
            return type.replace("xs:", "");
        } else {
            return type;
        }
    }


    private static Map<String, String> getValidXpath(Map<String, String> xpaths) {
        Map<String, String> validXpathMap = new HashMap<String, String>();
        for (Map.Entry<String, String> xpath : xpaths.entrySet()) {
            String[] xpathsKay = xpath.getKey().split("/");
            String dataType = "";

            if (xpathsKay.length > 2) {
                dataType = xpathsKay[0] + "/" + xpathsKay[1] + "/*";
                validXpathMap.put("column.xpath." + xpathsKay[1], dataType);
            } else {
                if (xpathsKay.length > 1) {
                    dataType = xpath.getKey() + "/" + "text()";
                    validXpathMap.put("column.xpath." + xpathsKay[1], dataType);
                } else if (xpathsKay.length == 1) {
                    dataType = rootElement + "/@" + xpathsKay[0];
                    validXpathMap.put("column.xpath." + xpathsKay[0], dataType);
                }


            }

        }
        return validXpathMap;
    }

    private static void setStartAndEndTag(Element element) {

        String rootElementname = element.getAttribute("name");
        rootElement = rootElementname;
        String startTag = "<" + rootElementname;
        String endTag = "</" + rootElementname + ">";

        XmlInputFormat.startTag = startTag;
        XmlInputFormat.endTag = endTag;
    }


}