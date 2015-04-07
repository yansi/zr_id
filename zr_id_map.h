#ifndef ZR_ID_MAP_H
#define ZR_ID_MAP_H

enum ZrSymbolType
{
    EX_SYMBOL,
    NX_SYMBOL,
    IB_SYMBOL,
    BB_SYMBOL,
    SYM_TYPE_LEN,
};

typedef struct ZrSecurityIDSet
{
    int secid;
    const char *sym_ar[SYM_TYPE_LEN];
} ZrID;     

struct ZrIDMap
{
public:    
    virtual int getSecID(ZrSymbolType stype, const char *sym) = 0;
    virtual int getZrID(int id, ZrID & ids) = 0;
    virtual int getZrID(ZrSymbolType stype, const char *sym, ZrID& ids) = 0;
    virtual int getUniverseSize() = 0;
    virtual int getUniverse(int len, ZrID *univ) = 0;
    virtual void Destroy() = 0;
};

extern "C" {
ZrIDMap* CreateZrIDMap(const char *host, int port, const char *usr, const char *pwd, const char *db) 
	throw(std::runtime_error, std::bad_alloc);
}

#endif // ZR_ID_MAP_H

