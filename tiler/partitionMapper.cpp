#include "hadoopgis.h"
#include "cmdline.h"
#include <string>

GeometryFactory *gf = NULL;
WKTReader *wkt_reader = NULL;
IStorageManager * storage = NULL;
ISpatialIndex * spidx = NULL;
map<id_type,string> id_tiles ;
vector<id_type> hits ; 
char * prefix;
char * filename;

int GEOM_IDX = -1;
map<int,Geometry*> geom_tiles;
map<int,long> count_tiles;
/* 
 * The program maps the input tsv data into corresponding partition 
 * (it adds the prefix partition id number at the beginning of the line)
 * */

RTree::Data* parseInputPolygon(Geometry *p, id_type m_id) {
    double low[2], high[2];
    const Envelope * env = p->getEnvelopeInternal();
    low [0] = env->getMinX();
    low [1] = env->getMinY();

    high [0] = env->getMaxX();
    high [1] = env->getMaxY();

    Region r(low, high, 2);

    return new RTree::Data(0, 0 , r, m_id);// store a zero size null poiter.
}


class GEOSDataStream : public IDataStream
{
    public:
	GEOSDataStream(map<int,Geometry*> * inputColl ) : m_pNext(0), len(0),m_id(0)
    {
	if (inputColl->empty())
	    throw Tools::IllegalArgumentException("Input size is ZERO.");
	shapes = inputColl;
	len = inputColl->size();
	iter = shapes->begin();
	readNextEntry();
    }
	virtual ~GEOSDataStream()
	{
	    if (m_pNext != 0) delete m_pNext;
	}

	virtual IData* getNext()
	{
	    if (m_pNext == 0) return 0;

	    RTree::Data* ret = m_pNext;
	    m_pNext = 0;
	    readNextEntry();
	    return ret;
	}

	virtual bool hasNext()
	{
	    return (m_pNext != 0);
	}

	virtual uint32_t size()
	{
	    return len;
	    //throw Tools::NotSupportedException("Operation not supported.");
	}

	virtual void rewind()
	{
	    if (m_pNext != 0)
	    {
		delete m_pNext;
		m_pNext = 0;
	    }

	    m_id  = 0;
	    iter = shapes->begin();
	    readNextEntry();
	}

	void readNextEntry()
	{
	    if (iter != shapes->end())
	    {
		//std::cerr<< "readNextEntry m_id == " << m_id << std::endl;
		m_id = iter->first;
		m_pNext = parseInputPolygon(iter->second, m_id);
		iter++;
	    }
	}

	RTree::Data* m_pNext;
	map<int,Geometry*> * shapes; 
	map<int,Geometry*>::iterator iter; 

	int len;
	id_type m_id;
};


class MyVisitor : public IVisitor
{
    public:
	void visitNode(const INode& n) {}
	void visitData(std::string &s) {}

	void visitData(const IData& d)
	{
	    hits.push_back(d.getIdentifier());
	    //std::cout << d.getIdentifier()<< std::endl;
	}

	void visitData(std::vector<const IData*>& v) {}
	void visitData(std::vector<uint32_t>& v){}
};



void doQuery(Geometry* poly) {
    double low[2], high[2];
    const Envelope * env = poly->getEnvelopeInternal();

    low [0] = env->getMinX();
    low [1] = env->getMinY();

    high [0] = env->getMaxX();
    high [1] = env->getMaxY();

   // cerr << low[0] << TAB << low[1] << TAB << high[0] << TAB << high[1] << endl;

    Region r(low, high, 2);

    // clear the result container 
    hits.clear();

    MyVisitor vis ; 
    //spidx->containsWhatQuery(r, vis);
    spidx->intersectsWithQuery(r, vis);

}

vector<string> parse(string & line) {
    vector<string> tokens ;
    tokenize(line, tokens,TAB,true);
    return tokens;
}

void genTiles() {
    vector<Geometry*> tiles;
    string input_line2;
    stringstream ss;

    vector<string> fields;
    double min_x, min_y, max_x, max_y;
    id_type id;
//    cerr << glominx << TAB << glominy << TAB << glomaxx << TAB << glomaxy << endl;

    std::ifstream skewFile(filename);
    while (std::getline(skewFile, input_line2)) {
	fields = parse(input_line2);
        min_x = std::stod(fields[1]);
        min_y = std::stod(fields[2]);
        max_x = std::stod(fields[3]);
        max_y = std::stod(fields[4]);

	ss << shapebegin << min_x << SPACE << min_y << COMMA
	 << min_x << SPACE << max_y << COMMA
	 << max_x << SPACE << max_y << COMMA
	 << max_x << SPACE << min_y << COMMA
	 << min_x << SPACE << min_y << shapeend;

//	cerr << ss.str() << endl;
	id = std::strtoul(fields[0].c_str(), NULL, 0);
        string iddes = fields[0];
        geom_tiles[id] = wkt_reader->read(ss.str());
        count_tiles[id] = 0;
	id_tiles[id] = iddes;

	ss.str(string());

        fields.clear();
    }   
}


void freeObjects() {
    // garbage collection 
    delete wkt_reader ;
    delete gf ; 
    delete spidx;
    delete storage;
}

void emitHits(Geometry* poly, string input_line) {
   for (uint32_t i = 0 ; i < hits.size(); i++ ) 
    {
	// cerr << hits[i] << endl;
	/* The first id of the the intersecting tile is used to 
 	 *
 	 * determine the name of the output file*/
	cout << hits[i] << TAB << hits[i]  << TAB 
	// << id_tiles[hits[i]] << TAB 
	<< input_line <<  endl ;
	count_tiles[ hits[i] ]++;
    }
}


bool buildIndex() {
    // build spatial index on tile boundaries 
    id_type  indexIdentifier;
    GEOSDataStream stream(&geom_tiles);
    storage = StorageManager::createNewMemoryStorageManager();
    spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
	    FillFactor,
	    IndexCapacity,
	    LeafCapacity,
	    2, 
	    RTree::RV_RSTAR, indexIdentifier);

    // Error checking 
    return spidx->isIndexValid();
}

int main(int argc, char **argv) {

  if (argc != 3) {
     cerr << "ERROR: Not enough arguments. Usage: " << argv[0]
            << " [geomd_id] [partition_file]" << endl;
     return -1;
  }
  //int uid_idx  = args_info.uid_arg;
  GEOM_IDX = atoi(argv[1]) - 1;
  filename = argv[2];

  /* 
     cerr << "min_x "<< min_x << endl; 
     cerr << "max_x "<< max_x << endl; 
     cerr << "min_y "<< min_y << endl; 
     cerr << "max_y "<< max_y << endl; 
     cerr << "x_split "<< x_split << endl; 
     cerr << "y_split "<< y_split << endl; 
     */

  // initlize the GEOS ibjects
  gf = new GeometryFactory(new PrecisionModel(),0);
  wkt_reader= new WKTReader(gf);


  // process input data 
  // map<int,Geometry*> geom_polygons;
  string input_line;
  vector<string> fields;
  cerr << "Reading input from stdin..." <<endl; 
  id_type id = 0; 
  Geometry* geom ; 

  genTiles();

  bool ret = buildIndex();
  if (ret == false) {
    cerr << "ERROR: Index building on tile structure has failed ." << std::endl;
    return 1 ;
  }
  else 
#ifndef NDEBUG  
    cerr << "GRIDIndex Generated successfully." << endl;
#endif



  while(cin && getline(cin, input_line) && !cin.eof()){
    fields = parse(input_line);

    if (fields[GEOM_IDX].length() <2 )
    {
#ifndef NDEBUG
      cerr << "skipping record [" << id <<"]"<< endl;
#endif
      continue ;  // skip lines which has empty geometry
    }
    // try {
    geom = wkt_reader->read(fields[GEOM_IDX]);
    //}
    /*catch (...)
      {
      cerr << "WARNING: Record [id = " <<i << "] is not well formatted "<<endl;
      cerr << input_line << endl;
      continue ;
      }*/

     doQuery(geom);
     emitHits(geom, input_line);

     // Memory cleaning
     delete geom;
  }

 // cerr << "Number of tiles: " << geom_tiles.size() << endl;
  
  // build spatial index for input polygons 
  for (map<int,long>::iterator it = count_tiles.begin(); it != count_tiles.end(); ++it) {
    cout << "Stat" << TAB << it->first << TAB << it->second << endl;
  }
 

  for (map<int,Geometry*>::iterator it = geom_tiles.begin(); it != geom_tiles.end(); ++it) {
     delete(it->second);
  }
  count_tiles.clear();
  geom_tiles.clear();

  cout.flush();
  cerr.flush();
  freeObjects();
  return 0; // success
}

