// zr_id_map.cpp : Defines the exported functions for the DLL application.
//

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdexcept>
#include <string>
#include <unordered_map>

#include "mysql.h"
#include "zr_id_map.h"
#pragma comment(lib, "mysqlclient.lib")


class ZrIDMapImp : public ZrIDMap
{
public:    
	ZrIDMapImp(const char *host, int port, const char *usr, const char *pwd, const char *db) 
		throw(std::runtime_error, std::bad_alloc);
    ~ZrIDMapImp();

    int getSecID(ZrSymbolType stype, const char *sym);
    int getZrID(int id, ZrID & ids);
    int getZrID(ZrSymbolType stype, const char *sym, ZrID& ids);
    int getUniverseSize();
    int getUniverse(int len, ZrID *univ);
    void Destroy();
private:
    int max_secid;      
    char ***sym_mx;                              /* sparse matrix of symbols, row is sid, column is symbol_type */

    typedef std::unordered_map<std::string, int> ZrSymbolHash;
    ZrSymbolHash symhash_ar[SYM_TYPE_LEN];       /* array of symbol hash, one hashtable for each sym type */
        
private:  
	ZrIDMapImp();
    ZrIDMapImp(const ZrIDMap&);
    ZrIDMapImp& operator=(const ZrIDMap&);
};

ZrIDMapImp::ZrIDMapImp(const char *host, int port, const char *usr, const char *pwd, const char *db)
	throw(std::runtime_error, std::bad_alloc)
{
    MYSQL mysql;
    MYSQL_RES *rs;
    MYSQL_ROW row;
    mysql_init(&mysql); 

    if (0 == mysql_real_connect(&mysql, host, usr, pwd, db, port, NULL, 0)) { 
		std::runtime_error e(mysql_error(&mysql));		    
		mysql_close(&mysql); 
		throw e;
    }

    const char *sql = "select a.status, a.secid, a.ticker, b.symbol from attr_hist a, product b "
                      "where a.secid = b.secid and a.startdate <= current_date and "
                      "a.enddate > current_date order by a.secid desc";

    if (0 != mysql_query(&mysql, sql)) {
		std::string errMsg = mysql_error(&mysql);
		errMsg += "\n";
		errMsg += sql;
				
		mysql_close(&mysql); 

		throw std::runtime_error(errMsg);
    }

    int nrow = 0;
    rs = mysql_store_result(&mysql); 
    while ((row = mysql_fetch_row(rs))) {
        if (0==nrow) { 
            max_secid = atoi(row[1]);
            printf("max secid = %d\n", max_secid);
            sym_mx = (char ***) calloc(max_secid+1, sizeof(char **));
            if(0 == sym_mx) throw std::bad_alloc();
        }
        nrow++;
        printf( "%s,%s,%s,%s\n", row[0], row[1], row[2], row[3]);

        if (strcmp("RG", row[0])) { 
            fprintf(stderr, "not regular status for %s, %s, %s\n", row[1], row[2], row[3]);
            continue;
			        }
        int secid = atoi(row[1]);
        if (secid <= 0 || secid > max_secid) { 
            fprintf(stderr, "bad secid: %s, %s, %s\n", row[1], row[2], row[3]);
            continue;
        }

        sym_mx[secid] = (char **)calloc(SYM_TYPE_LEN, sizeof(char *));
        if(0 == sym_mx[secid]) throw std::bad_alloc();

        if(0 != row[2]) { 
            char nxsym[128];
            const char *exsym = row[2];
            _snprintf_s(nxsym, 128, "e%s", exsym);

            sym_mx[secid][EX_SYMBOL] = _strdup(exsym);
            sym_mx[secid][NX_SYMBOL] = _strdup(nxsym);

            symhash_ar[EX_SYMBOL][exsym] = secid;
            symhash_ar[NX_SYMBOL][nxsym] = secid;
        }        
		
		if(0 != row[3]) { 
            const char *ibsym = row[3];
            sym_mx[secid][IB_SYMBOL] = _strdup(ibsym);
            symhash_ar[IB_SYMBOL][ibsym] = secid;
        }
                
    }

    mysql_free_result(rs); 
    mysql_close(&mysql); 
}

ZrIDMapImp::~ZrIDMapImp()
{
    if(0 != sym_mx) { 
        for(int i = 0; i <= max_secid; i++) { 
            if(0 != sym_mx[i]) { 
                for(int k = 0; k < SYM_TYPE_LEN; k++) { 
                    if(0 != sym_mx[i][k]) { 
                       free(sym_mx[i][k]);
                       sym_mx[i][k] = 0;
                    } 
                }
                free(sym_mx[i]);
                sym_mx[i] = 0;
            }
        }
        free(sym_mx);
        sym_mx = 0;
        fprintf(stderr, "freed sym_mx.\n");
    }

}

int ZrIDMapImp::getSecID(ZrSymbolType stype, const char *sym)
{
    ZrSymbolHash::const_iterator it = symhash_ar[stype].find (sym);
    if(it != symhash_ar[stype].end()) { 
        return it->second;
    }
    return -1;
}

int ZrIDMapImp::getZrID(int id, ZrID & ids)
{
    if(id <= max_secid && id >= 0 && 0 != sym_mx[id]) { 
        for(int k = 0; k < SYM_TYPE_LEN; k++ ) { 
            ids.sym_ar[k] = sym_mx[id][k];
        }
        ids.secid = id;
        return 1;
    }

    return 0;
}

int ZrIDMapImp::getZrID(ZrSymbolType stype, const char *sym, ZrID& ids)
{
    ZrSymbolHash::const_iterator it = symhash_ar[stype].find(sym);
    if(it != symhash_ar[stype].end()) { 
        return getZrID(it->second, ids);
    }
    return 0;
}

int ZrIDMapImp::getUniverseSize(void)
{
    int usiz = 0;
    for(int i = 0; i <= max_secid; i++) { 
        sym_mx[i] && usiz++;
    }
    return usiz;
}

int ZrIDMapImp::getUniverse(int len, ZrID *univ)
{
    int usiz = 0;
    for(int i = 0; i <= max_secid; i++) {
        if(0 != sym_mx[i]) { 
            if(usiz >= len) { 
                fprintf(stderr, "::getUniverse container size %d less than univ size %d, stop\n", len, usiz+1);
                break;
            } else {
                univ[usiz].secid = i;
                for(int k = 0; k < SYM_TYPE_LEN; k++) { 
                    univ[usiz].sym_ar[k] = sym_mx[i][k];
                }
                usiz++;
            }            
        }
    }
    
    return usiz;
}

void ZrIDMapImp::Destroy() {
	delete this;
}
 
extern "C" {
		ZrIDMap* CreateZrIDMap(const char *host, int port, const char *usr, const char *pwd, const char *db) 
			throw(std::runtime_error, std::bad_alloc)
		{
			try { 
				return new ZrIDMapImp(host, port, usr, pwd, db);
			} catch (std::runtime_error &e) { 
				throw(e);
			} catch (std::bad_alloc &e1) {
				throw(e1);
			}
		}
}

