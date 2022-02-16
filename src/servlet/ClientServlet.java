package servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.json.JSONObject;

/**
 * Servlet implementation class ClientServlet
 */
@WebServlet("/ClientServlet")
public class ClientServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public ClientServlet() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	    
	    String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
	    
	    System.out.println(reqBody);
	    
	    JSONObject json = new JSONObject(reqBody);
	    
	    String rdf4jServer = json.get("server").toString();
	    String repoID = json.get("id").toString();
	    String query = json.get("query").toString();
	    
	    Repository repo = new HTTPRepository(rdf4jServer, repoID);
	    
	    HashMap<String, List<String>> results = new HashMap<>();
	    
	    PrintWriter out = response.getWriter();
	    
	    try { RepositoryConnection con = repo.getConnection();
	    
	            
    	        try { // First try tuple query (SELECT)
    	            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
    	            TupleQueryResult result = tupleQuery.evaluate();
    	            try {
    	                List<String> bindingNames = result.getBindingNames();
    	                
    	                for (int i = 0; i < bindingNames.size(); i++) {
    	                    List<String> bindingResults = new ArrayList<>();
    	                    results.put(bindingNames.get(i), bindingResults);
    	                }
    	                
    	                while (result.hasNext()) {
    	                    BindingSet bindingSet = result.next();
    	                       	                 
    	                    
    	                    for (int i = 0; i < bindingNames.size(); i++) {
    	                        String binding = bindingNames.get(i);
    	                        Value val = bindingSet.getValue(binding);
    	                        if (val != null) {
    	                            results.get(binding).add(val.stringValue());
    	                        }
    	                        
    	                    }
    	                    
    	                    
    	                }
    	            } finally { result.close(); }
    	        } catch (RDF4JException e1) {
    	            
    	            try { //Next try graph query (CONSTRUCT or DESCRIBE)
    	                
    	                Model m = Repositories.graphQuery(repo, query, r -> QueryResults.asModel(r));
    	                
    	                final String SUBJECT = "subject";
    	                final String PREDICATE = "predicate";
    	                final String OBJECT = "object";
    	                
    	                results.put(SUBJECT, new ArrayList<String>());
    	                results.put(PREDICATE, new ArrayList<String>());
    	                results.put(OBJECT, new ArrayList<String>());
    	                
    	                for (Statement s : m) {
    	                    results.get(SUBJECT).add(s.getSubject().stringValue());
    	                    results.get(PREDICATE).add(s.getPredicate().stringValue());
    	                    results.get(OBJECT).add(s.getObject().stringValue());
    	                }
                        
                        
                    } catch (RDF4JException e2) {
                        
                        try { //Finally try boolean query (ASK)
                            BooleanQuery boolQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL, query);
                            boolean result = boolQuery.evaluate();
                            
                            results.put("result", new ArrayList<String>());
                            
                            if (result) {                       
                                results.get("result").add("yes");
                            }
                            else {
                                results.get("result").add("no");
                            }
                            
                            
                        } catch (RDF4JException e3) { //None of the query types succeeded
                            
                            System.err.println(e3.getMessage());
                            response.setStatus(400);
                            if (e3.getMessage() != null) {
                                out.write(e3.getMessage());
                            }
                            else {
                                out.write("Malformed query");
                            }
                            out.flush();
                            out.close();
                            return;
                            
                        }
                        
                    }
    	            
    	        }
    	        finally { con.close(); }
	        
	    } catch (RDF4JException e) { //Connection to repository failed
	        System.err.println(e.getMessage());
	        response.setStatus(500);
	        if (e.getMessage() != null) {
	            out.write(e.getMessage());
	        }
	        else {
	            out.write("Connection failed");
	        }
	        out.flush();
	        out.close();
	        return;
	        
	    }
	    System.out.println(results.toString());
	    
	    JSONObject resultjson = new JSONObject(results);
	    
	    out.write(resultjson.toString());
	    out.flush();
	    out.close();
	}

}
