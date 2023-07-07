package ictjob.search.lucene;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ictjob.lang.Pair;
import ictjob.search.Globals;
import ictjob.search.beans.SearchParams;
import ictjob.utils.lucene.AnalyzerUtils;

/**
 * Convert a SearchQuery to the corresponding BooleanQuery.
 * Creating a new BooleanQuery() is deprecated in Lucene 5.3.0 and deleted in 6.0.0,
 * it is replaced with BooleanQuery.Builder.
 */

public class SearchQueryToBooleanQueryConverter
{
  private static final Logger LOGGER  = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String LINESEP = System.lineSeparator();

//    private Map<String, List<String>>                 searchFieldNameToCRITERIONOperandList;
//    private Map<String, List<String>>                 searchFieldNameToTEXTOperandList;
//    private Map<String, List<Pair<Integer, Integer>>> searchFieldNameToRANGEPairList;
//
//    BooleanQuery convert(SearchParams searchParams) {
//        BooleanQuery bqQuery = new BooleanQuery();
//        walkTreeToBuildBooleanQuery(searchParams.getIndexLanguageId(), searchParams.getSearchQuery(), bqQuery);
//        return bqQuery;
//    }
  private final String                   indexLanguageId;
  private final SearchQuery              searchQuery;
  private final int                      phraseQuerySlop;
  private final Map<String, List<Float>> searchFieldCRITERIONToBoostList = new HashMap<>();
  private final Map<String, List<Float>> searchFieldRANGE1ToBoostList    = new HashMap<>();
  private final Map<String, List<Float>> searchFieldRANGE2ToBoostList    = new HashMap<>();
  private final Map<String, List<Float>> searchFieldRANGE3ToBoostList    = new HashMap<>();
  
  public SearchQueryToBooleanQueryConverter(SearchParams searchParams) {
    this.indexLanguageId = searchParams.getIndexLanguageId();
    this.searchQuery     = searchParams.getSearchQuery();
    this.phraseQuerySlop = searchParams.getPhraseQuerySlop();
  }
  
  public BooleanQuery convert() {
    BooleanQuery bqQuery = new BooleanQuery();
    walkTreeToBuildBooleanQuery(indexLanguageId, searchQuery, bqQuery);
    return bqQuery;
  }
  public BooleanQuery convertTEXT_ONLY() {
    BooleanQuery bqQuery          = new BooleanQuery();
    walkTreeToBuildBooleanQueryTEXT_ONLY(indexLanguageId, searchQuery, bqQuery);
    return bqQuery;
  }

  void walkTreeToBuildBooleanQuery(String indexLanguageId, SearchQuery searchQueryParent, BooleanQuery bQParent) {
      // comment MainOne
      // comment MainTwo
    /* The only particular case: the searchQuery does start with a SearchQueryOperator,
     * not with a SearchQueryAggregator;
     * design decision: the MUST clause is added.
     */
    if (searchQueryParent.getAggregator() == null) {
      bQParent.add(processOperatorLeaf(indexLanguageId, searchQueryParent), Occur.MUST);
    }

    /*
     * General tree: aggregator with at least 1 child, either a SearchQueryAggregator or a SearchQueryOperator.
     */
    else {
      /*
       * aggregator OPTIONAL, i.e. SHOULD,
       * add a MatchAllDocsQuery (*:*) as first boolean child of the boolean parent, with a boost of 0,
       * it is like adding an artificial sibling to the children of this OPTIONAL parent;
       * afterwards process all the actual children of this OPTIONAL parent with their own boost
       * 
       * aggregator NOT cannot be used as the root aggregator of the tree:
       * therefore it works to place "*:*" before the global query and to keep the operator "-" where there is a "NOT",
       * but, the cases where NOT is deeply immersed in a complex query should be tricky,
       * and although *:* is slow because the search engine has to look through each document,
       * I still translate "NOT" with "*:* -" everywhere it appears in the tree
       */
      if (searchQueryParent.getAggregator()==SearchQueryAggregator.OPTIONAL) {
        MatchAllDocsQuery matchAllDocsQuery = new MatchAllDocsQuery();
        matchAllDocsQuery.setBoost(0f);
        bQParent.add(matchAllDocsQuery, Occur.SHOULD);
      } else if (searchQueryParent.getAggregator()==SearchQueryAggregator.NOT) {
        MatchAllDocsQuery matchAllDocsQuery = new MatchAllDocsQuery();
        // default score of MatchAllDocsQuery = 1.0
        bQParent.add(matchAllDocsQuery, Occur.SHOULD); // MUST??
      } else {
      }
      
      for (SearchQuery searchQueryChild : searchQueryParent.getChildren()) {
        /* this child is an operator SearchQuery, it is a leaf SearchQuery */
        if (searchQueryChild.getAggregator() == null) {
          bQParent.add(processOperatorLeaf(indexLanguageId, searchQueryChild), searchQueryParent.getAggregator().getOccur());
        }
        /* this child is an aggregator SearchQuery, thus recursion */
        else {
          BooleanQuery bQChild = new BooleanQuery();
          walkTreeToBuildBooleanQuery(indexLanguageId, searchQueryChild, bQChild);
          bQParent.add(bQChild, searchQueryParent.getAggregator().getOccur());
        }
      }
    }
  }

  void walkTreeToBuildBooleanQueryTEXT_ONLY(String indexLanguageId, SearchQuery searchQueryParent, BooleanQuery bQParent) {
    /* The only particular case: the searchQuery does start with a SearchQueryOperator,
     * not with a SearchQueryAggregator;
     * design decision: the MUST clause is added.
     */
//    searchQueryParent.aggregator = SearchQueryAggregator.OR;
    if (searchQueryParent.getAggregator() == null) {
      bQParent.add(processOperatorLeaf(indexLanguageId, searchQueryParent), Occur.MUST);
    }

    /*
     * General tree: aggregator with at least 1 child, either a SearchQueryAggregator or a SearchQueryOperator.
     */
    else {
      /*
       * aggregator OPTIONAL, i.e. SHOULD,
       * add a MatchAllDocsQuery (*:*) as first boolean child of the boolean parent, with a boost of 0,
       * it is like adding an artificial sibling to the children of this OPTIONAL parent;
       * afterwards process all the actual children of this OPTIONAL parent with their own boost
       * 
       * aggregator NOT cannot be used as the root aggregator of the tree:
       * therefore it works to place "*:*" before the global query and to keep the operator "-" where there is a "NOT",
       * but, the cases where NOT is deeply immersed in a complex query should be tricky,
       * and although *:* is slow because the search engine has to look through each document,
       * I still translate "NOT" with "*:* -" everywhere it appears in the tree
       */
      if (searchQueryParent.getAggregator()==SearchQueryAggregator.OPTIONAL) {
        MatchAllDocsQuery matchAllDocsQuery = new MatchAllDocsQuery();
        matchAllDocsQuery.setBoost(0f);
        bQParent.add(matchAllDocsQuery, Occur.SHOULD);
      } else if (searchQueryParent.getAggregator()==SearchQueryAggregator.NOT) {
        MatchAllDocsQuery matchAllDocsQuery = new MatchAllDocsQuery();
        // default score of MatchAllDocsQuery = 1.0
        bQParent.add(matchAllDocsQuery, Occur.SHOULD); // MUST??
      } else {
      }
      
      for (SearchQuery searchQueryChild : searchQueryParent.getChildren()) {
        /* this child is an operator SearchQuery, it is a leaf SearchQuery */
        if (searchQueryChild.getAggregator() == null) {
          bQParent.add(processOperatorLeafTEXT_ONLY(indexLanguageId, searchQueryChild), searchQueryParent.getAggregator().getOccur());
        }
        /* this child is an aggregator SearchQuery, thus recursion */
        else {
          BooleanQuery bQChild = new BooleanQuery();
          walkTreeToBuildBooleanQueryTEXT_ONLY(indexLanguageId, searchQueryChild, bQChild);
          bQParent.add(bQChild, searchQueryParent.getAggregator().getOccur());
        }
      }
    }
  }

  /**
   * 
   * @param indexLanguageId
   * @param searchQuery      1 operator SearchQuery = 1 leaf SearchQuery
   * @return
   */
  private Query processOperatorLeaf(String indexLanguageId, SearchQuery searchQuery) {
    Query                 ret                 = null;
    SearchQueryAggregator parentAggregator    = searchQuery.getParent().getAggregator();
    SearchQueryOperator   searchQueryOperator = searchQuery.getOperator();
    ISearchField          searchField         = searchQuery.getSearchField();
    MyFieldType           myFieldType         = searchField.getMyFieldType();
    String                searchFieldName     = searchQuery.getSearchFieldNameSuffixed();
    float                 queryBoost          = searchField.getQueryBoost();

    switch (searchQueryOperator) {
      case EQUALS:
        if (parentAggregator!=SearchQueryAggregator.NOT && myFieldType==MyFieldType.CRITERION) {
          if (searchFieldCRITERIONToBoostList.get(searchFieldName) == null) {
            searchFieldCRITERIONToBoostList.put(searchFieldName, new ArrayList<>());
          }
        searchFieldCRITERIONToBoostList.get(searchFieldName).add(queryBoost);
        }
//        ret = new TermQuery(new Term(searchFieldName, searchQuery.getOneOperand()));
//        System.out.println("in processOperatorLeaf");
//        ret = getCriterionQuery(searchFieldName, searchQuery);
        break;
      case BETWEEN:
        if (parentAggregator!=SearchQueryAggregator.NOT) {
          switch (myFieldType) {
            case RANGE1:
              if (searchFieldRANGE1ToBoostList.get(searchFieldName) == null) {
                searchFieldRANGE1ToBoostList.put(searchFieldName, new ArrayList<>());
              }
              searchFieldRANGE1ToBoostList.get(searchFieldName).add(queryBoost);
              break;
            case RANGE2:
              if (searchFieldRANGE2ToBoostList.get(searchFieldName) == null) {
                searchFieldRANGE2ToBoostList.put(searchFieldName, new ArrayList<>());
              }
              // comment MainThree
              // comment MainFour
              searchFieldRANGE2ToBoostList.get(searchFieldName).add(queryBoost);
              break;
            case RANGE3:
              if (searchFieldRANGE3ToBoostList.get(searchFieldName) == null) {
                searchFieldRANGE3ToBoostList.put(searchFieldName, new ArrayList<>());
              }
              searchFieldRANGE3ToBoostList.get(searchFieldName).add(queryBoost);
              break;
            default:
              break;
          }
        }
        ret = getNumericRangeQuery(searchFieldName, searchQuery);
        break;
      case CONTAINS:
        String searchQueryString = searchQuery.getOneOperand().trim();                // no escape at all
        //searchQueryString = QueryParserUtil.escape(searchQueryString);                // Lucene escape
        //searchQueryString = AnalyzerUtils.getInstance().myEscape(searchQueryString);  // myEscape
        searchQueryString = AnalyzerUtils.INSTANCE.stripAccentAndLowercase(searchQueryString.toLowerCase());
        List<Pair<String, Integer>> termPositionPairList = getTermPositionPairList(searchQueryString, indexLanguageId);
        /* one word */
        if (termPositionPairList.size() == 1) {
          ret = new TermQuery(new Term(searchFieldName, termPositionPairList.get(0).getLeft()));
        }
        /* more than one word i.e. a phrase */
        else {
          PhraseQuery phraseQuery = new PhraseQuery();
          phraseQuery.setSlop(phraseQuerySlop);
          for (Pair<String, Integer> termPositionPair : termPositionPairList) {
              phraseQuery.add(new Term(searchFieldName, termPositionPair.getLeft()), termPositionPair.getRight());
          }
          ret = phraseQuery;
        }
        break;
      default:
        break;
    }
    ret.setBoost(queryBoost);
    return ret;
  }

  /**
   * 
   * @param indexLanguageId
   * @param searchQuery      1 operator SearchQuery = 1 leaf SearchQuery
   * @return
   */
  private Query processOperatorLeafTEXT_ONLY(String indexLanguageId, SearchQuery searchQuery) {
    Query                 ret                 = null;
    SearchQueryAggregator parentAggregator    = searchQuery.getParent().getAggregator();
    SearchQueryOperator   searchQueryOperator = searchQuery.getOperator();
    ISearchField          searchField         = searchQuery.getSearchField();
    MyFieldType           myFieldType         = searchField.getMyFieldType();
    String                searchFieldName     = searchQuery.getSearchFieldNameSuffixed();
    float                 queryBoost          = searchField.getQueryBoost();

    switch (searchQueryOperator) {
      case EQUALS:
        ret = new MatchNoDocsQuery();
        break;
      case BETWEEN:
        ret = new MatchNoDocsQuery();
        break;
      case CONTAINS:
        String searchQueryString = searchQuery.getOneOperand().trim();                // no escape at all
        //searchQueryString = QueryParserUtil.escape(searchQueryString);                // Lucene escape
        //searchQueryString = AnalyzerUtils.getInstance().myEscape(searchQueryString);  // myEscape
        searchQueryString = AnalyzerUtils.INSTANCE.stripAccentAndLowercase(searchQueryString.toLowerCase());
        List<Pair<String, Integer>> termPositionPairList = getTermPositionPairList(searchQueryString, indexLanguageId);
        /* one word */
        if (termPositionPairList.size() == 1) {
          ret = new TermQuery(new Term(searchFieldName, termPositionPairList.get(0).getLeft()));
        }
        /* more than one word i.e. a phrase */
        else {
          PhraseQuery phraseQuery = new PhraseQuery();
          phraseQuery.setSlop(phraseQuerySlop);
          for (Pair<String, Integer> termPositionPair : termPositionPairList) {
              phraseQuery.add(new Term(searchFieldName, termPositionPair.getLeft()), termPositionPair.getRight());
          }
          ret = phraseQuery;
        }
        break;
      default:
        break;
    }
    Float b = myFieldType==MyFieldType.TEXT ? queryBoost : 0.0f;
    ret.setBoost(b);
    return ret;
  }

  /**
   * 
   * @param searchQueryString  text of an CONTAINS operator SearchQuery
   * @param languageId the key of the LANGUAGE2ANALYZER map is -languageId to get
   *                   the same analyzer as for indexing but without the synonym filter 
   * @return
   */
  private List<Pair<String, Integer>> getTermPositionPairList(String searchQueryText, String languageId) {
    List<Pair<String, Integer>> ret      = new ArrayList<>();
    /* MyEnglishAnalyzer without synonym filter */
    Analyzer                    analyzer = AnalyzerUtils.INSTANCE.getLanguageToMyAnalyzer().get("-" + languageId);
    try (TokenStream tokenStream = analyzer.tokenStream("searchQueryText", searchQueryText)) {
      CharTermAttribute          charTermAttribute          = tokenStream.addAttribute(CharTermAttribute.class);
      PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);
      tokenStream.reset();  // Resets this stream to the beginning. (Required)
      int position = -1;
      while (tokenStream.incrementToken()) {
        /* Use AttributeSource.reflectAsString(boolean) for token stream debugging. */
        LOGGER.debug("token: {}", tokenStream.reflectAsString(true)); 
        position += positionIncrementAttribute.getPositionIncrement();
        ret.add(new Pair<>(charTermAttribute.toString(), position));
      }
      tokenStream.end();  // Perform end-of-stream operations, e.g. set the final offset
    } catch (IOException ex) {
      LOGGER.error("exception when analyzing the query string{}{} in the language {}", LINESEP, searchQueryText, languageId, ex);
      throw new UncheckedIOException(ex);
    }
    return ret;
  }

  private Query getCriterionQuery(String searchFieldName, SearchQuery searchQuery) {
    Query subQuery = new TermQuery(new Term(searchFieldName, searchQuery.getOneOperand()));
    System.out.println("in getCriterionQuery");
    return Globals.INSTANCE.getIEntityCriterionScoreQuery(subQuery);
  }

  private Query getNumericRangeQuery(String searchFieldName, SearchQuery searchQuery) {
    if (searchFieldName.endsWith("Epoch")) {
      NumericRangeQuery<Long> numericRangeQueryLong = NumericRangeQuery.newLongRange(searchFieldName, searchQuery.getTwoLongOperandsMin(), searchQuery.getTwoLongOperandsMax(), true, true);
      return Globals.INSTANCE.getIEntityRangeScoreQuery(numericRangeQueryLong);
    } else {
      NumericRangeQuery<Integer> numericRangeQueryInteger = NumericRangeQuery.newIntRange (searchFieldName, searchQuery.getTwoIntOperandsMin(),  searchQuery.getTwoIntOperandsMax(),  true, true);
      return Globals.INSTANCE.getIEntityRangeScoreQuery(numericRangeQueryInteger);
    }
//        Class<? extends Number> clazz = null;
//        if (searchQuery.getTwoIntOperandsMin()!=0 && searchQuery.getTwoIntOperandsMax()!=0) {
//            clazz = Integer.class;
//        } else if (searchQuery.getTwoLongOperandsMin()!=0 && searchQuery.getTwoLongOperandsMax()!=0) {
//            clazz = Long.class;
//        } else {
//            LOGGER.error("exception when getting the NumericRangeQuery: Integer or Long type parameter required");
//            throw new InvalidStateException();
//        }
//        
//        if  (clazz.equals(Integer.class)) {
//            NumericRangeQuery<Integer> numericRangeQueryInteger = NumericRangeQuery.newIntRange (searchFieldName, searchQuery.getTwoIntOperandsMin(),  searchQuery.getTwoIntOperandsMax(),  true, true);
//            ret = Globals.INSTANCE.getIEntityRangeQuery(numericRangeQueryInteger, clazz);
//        } else {
//            NumericRangeQuery<Long> numericRangeQueryLong = NumericRangeQuery.newLongRange(searchFieldName, searchQuery.getTwoLongOperandsMin(), searchQuery.getTwoLongOperandsMax(), true, true);
//            ret = Globals.INSTANCE.getIEntityRangeQuery(numericRangeQueryLong, clazz);
//        }
//        
//        return ret;
  }
  
  public Map<String, List<Float>> getSearchFieldCRITERIONToBoostList() { return searchFieldCRITERIONToBoostList; }
  public Map<String, List<Float>> getSearchFieldRANGE1ToBoostList()    { return searchFieldRANGE1ToBoostList; }
  public Map<String, List<Float>> getSearchFieldRANGE2ToBoostList()    { return searchFieldRANGE2ToBoostList; }
  public Map<String, List<Float>> getSearchFieldRANGE3ToBoostList()    { return searchFieldRANGE3ToBoostList; }
}
