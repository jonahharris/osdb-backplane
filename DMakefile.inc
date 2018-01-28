# DMakefile
#
# $Backplane: rdbms/DMakefile.inc,v 1.24 2003/05/31 19:47:48 dillon Exp $

INCLUDE= -I$(TOPDIR)
MODULELIBS ?= dbcore dbtypes client


# DMakefile.inc		Global DMakefile to support source tree
#
# Includes and the reasons for them:
#	TOPDIR		access to the global include hierarchy
#	TOPDIR/objs	access to other module's prototypes header file
#	OD		access to prototypes header file in object dir
#	INCLUDE		relative access to other module's .h files
#

.ifuser root
INSTALLDIR ?= /usr/local/backplane
.else
INSTALLDIR ?= $(HOME)/backplane
.endif
DRDBMS ?= $(INSTALLDIR)/drdbms
CC= cc
REPLICATOR= $(INSTALLDIR)/bin/replicator
LMODULE ?= $(MODULE)
OD= $(TOPDIR)objs/$(LMODULE)
BIN= $(TOPDIR)bin
LIB= $(TOPDIR)lib
PROTOS= $(OD)/$(MODULE)-protos.h
#DEPEND= $(OD)/$(MODULE)-depend
EXPORTS= $(OD)/$(MODULE)-exports.h
MANS ?=
DOCFILES ?=
INCLUDE ?=
VERS = 1.05
HEADERS ?=
MODULELIBS ?=
EXTRALIBS ?=
EXTRADEFS ?=
EXTRALFLAGS ?=
EXTRACFLAGS ?=
COPTFLAGS ?= -g
CFLAGS= -pipe $(COPTFLAGS) -O -I. -I$(TOPDIR) -I$(TOPDIR)rdbms/ -I$(TOPDIR)objs -I$(OD) $(INCLUDE) -DBACKPLANE_VERSION=201 -D_BACKPLANE_BASE=$(INSTALLDIR)/ -Wall -Wstrict-prototypes $(EXTRACFLAGS)
.ifos linux
CFLAGS= $(CFLAGS) -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
.endif
PLATFORM= `uname -m`
CGIBIN= $(INSTALLDIR)/apache/cgi-bin/
INSTALLBIN= $(INSTALLDIR)/bin
INSTALLLIB= $(INSTALLDIR)/lib
INSTALLINC= $(INSTALLDIR)/include/lib$(MODULE)
INSTALLINC2= $(INSTALLDIR)/include/$(MODULE)
INSTALLETC= $(INSTALLDIR)/etc
INSTALLLIBEXEC= $(INSTALLDIR)/libexec
INSTALLLOG= $(INSTALLDIR)/log
INSTALLREADME= $(INSTALLDIR)/README
INSTALLCOPYRIGHT= $(INSTALLDIR)/COPYRIGHT
INSTALLMAN= $(INSTALLDIR)/man/$(MODULE)
INSTALLDOCS= $(INSTALLDIR)/docs
INSTALLMANFILES= $(MANS:"*":"$(INSTALLMAN)/*")
INSTALLDOCFILES= $(DOCFILES:"*":"$(INSTALLDOCS)/*")
EXEMODULE ?= $(MODULE)

OPENSSL= openssl
PRIVATEKEY= key.pem
PUBLICKEY= key.pub

LD ?=
ULD= $(LD) $(TOPDIR)objs
LIBS ?= threads support 
LFLAGS= $(ULD:"*":"-L*") $(EXTRALFLAGS) $(MODULELIBS:"*":"-l*") $(EXTRALIBS:"*":"-l*") $(LIBS:"*":"-l*") -lm
#LIBARCHIVES= $(LIBS:"*":"$(LD)/lib*.a")

SRCS ?=
OBJS= $(SRCS:"*.?":"$(OD)/$(MODULE)_*.o")
UTILS= $(SRCS:"*.?":"$(BIN)/*")
IUTILS= $(SRCS:"*.?":"$(INSTALLBIN)/*")
LIBNAME= $(TOPDIR)objs/lib$(MODULE).a

CUTILS= $(SRCS:"*.c":"$(BIN)/*")
EUTILS= $(SRCS:"*.e":"$(BIN)/*")
CSRCS= $(SRCS:"*.c")
ESRCS= $(SRCS:"*.e")
ETMPS= $(ESRCS:"*.e":"$(OD)/$(MODULE)_*.c")
COBJS= $(SRCS:"*.c":"$(OD)/$(MODULE)_*.o")
EOBJS= $(SRCS:"*.e":"$(OD)/$(MODULE)_*.o")

EXE= $(BIN)/$(EXEMODULE)
IEXE= $(INSTALLBIN)/$(MODULE)
ILIB= $(INSTALLLIB)/lib$(MODULE).a

_lib: $(OD) $(LIBNAME)

_exe: $(OD) $(BIN) $(EXE)

_utils: $(OD) $(BIN) $(UTILS)

_relink_lib:	__clean_lib _lib

_relink_exe:	__clean_exe _exe

_relink_utils:	__clean_utils _utils

__clean_lib:
	rm -f $(LIBNAME)

__clean_exe:
	rm -f $(EXE)

__clean_utils:
	rm -f $(UTILS)

_subdirs:
	@for i in $(MODULES); do (cd $i; echo $i; dxmake -q all) || exit 1; done

_cleansubdirs:
	@for i in $(MODULES); do 		\
	    if [ -d $i ]; then			\
		echo "Cleaning $i";		\
		(cd $i; dxmake -q clean);	\
	    else				\
		echo "Cannot find directory $i";\
	    fi;					\
	    if [ -d $(TOPDIR)objs/$i ]; then 	\
		rmdir $(TOPDIR)objs/$i; 	\
	    fi;					\
	done

_cleanexe:
	rm -f $(OBJS) $(EXE) $(PROTOS) $(EXPORTS) $(ETMPS)
	if [ -d $(OD) ]; then rmdir $(OD); fi

_cleanutils:
	rm -f $(OBJS) $(EXE) $(UTILS) $(PROTOS) $(EXPORTS) $(ETMPS)
	if [ -d $(OD) ]; then rmdir $(OD); fi

_cleanlib:
	rm -f $(OBJS) $(LIBNAME) $(PROTOS) $(EXPORTS) $(ETMPS)
	if [ -d $(OD) ]; then rmdir $(OD); fi

_dependsubdirs:
	@for i in $(MODULES); do 		\
	    if [ -d $i ]; then			\
		echo "Depend $i";		\
		(cd $i; dxmake -q depend);	\
	    else				\
		echo "Cannot find directory $i";\
	    fi;					\
	done

_dependexe:
	# TODO

_dependutils:
	# TODO

_dependlib:
	# TODO

_installsubdirs:
	@for i in $(MODULES); do		\
	    if [ -d $i ]; then			\
		echo "Installing $i";		\
		(cd $i; dxmake -DINSTALLDIR=$(INSTALLDIR) -q install);	\
	    else				\
		echo "Cannot find directory $i";\
	    fi;					\
	done

_installutils: $(INSTALLBIN) $(IUTILS)

_installexe: $(INSTALLBIN) $(IEXE)

INST_EXPS= $(INSTALLINC)/$(MODULE)-exports.h
INST_INCLS= $(HEADERS:"*":"$(INSTALLINC)/*")
INST_INCLS2= $(HEADERS:"*":"$(INSTALLINC2)/*")

_installlib: $(INSTALLLIB) $(INSTALLINC) $(ILIB) $(INST_EXPS) $(INST_INCLS)

_installheaders: $(INSTALLLIB) $(INSTALLINC2) $(INST_INCLS2)

_installman: $(INSTALLMAN) $(INSTALLMANFILES)

_installdocs: $(INSTALLDOCS) $(INSTALLDOCFILES)

#$(INST_EXPS) : $(EXPORTS)

$(INST_EXPS) : $(SRCS)
	cp $(EXPORTS) %(left)
	chmod 644 %(left)

$(INST_INCLS) : $(HEADERS)
	cp %(right) %(left)
	chmod 644 %(left)

$(INST_INCLS2) : $(HEADERS)
	cp %(right) %(left)
	chmod 644 %(left)

$(DRDBMS) : $(DRDBMS)
	mkdir -p %(left)

$(OD) : $(OD)
	mkdir -p %(left)

$(BIN) : $(BIN)
	mkdir -p %(left)

$(LIB) : $(LIB)
	mkdir -p %(left)

$(INSTALLBIN) : $(INSTALLBIN)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLLOG) : $(INSTALLLOG)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLREADME) : $(TOPDIR)README
	cp %(right) %(left)
	chmod 644 %(left)

$(INSTALLCOPYRIGHT) : $(TOPDIR)COPYRIGHT
	cp %(right) %(left)
	chmod 644 %(left)

$(INSTALLMAN) : $(INSTALLMAN)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLDOCS) : $(INSTALLDOCS)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLMANFILES) : $(MANS)
	cp %(right) %(left)
	chmod 644 %(left)

$(INSTALLDOCFILES) : $(DOCFILES)
	cp %(right) %(left)
	chmod 644 %(left)

$(INSTALLLIB) : $(INSTALLLIB)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLINC) : $(INSTALLINC)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLINC2) : $(INSTALLINC2)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLETC) : $(INSTALLETC)
	mkdir -p %(left)
	chmod 755 %(left)

$(INSTALLLIBEXEC) : $(INSTALLLIBEXEC)
	mkdir -p %(left)
	chmod 755 %(left)

$(PROTOS) : $(SRCS)
	@rm -f %(left).new
	cat %(right) | sed -n -e '/^Prototype/s//extern/p' -e '/^Export/s//extern/p' > %(left).new
	cmp %(left) %(left).new || mv -f %(left).new %(left)
	@rm -f %(left).new

#$(DEPEND) : $(SRCS)
#	@rm -f %(left).new
#	$(CC) $(CFLAGS) $(EXTRADEFS) %(right) -M | \
#		sed -e "s=^.*:=$(OD)/$(MODULE)_&=" > %(left).new
#	cmp %(left) %(left).new || mv -f %(left).new %(left)
#	@rm -f %(left).new

$(EXPORTS) : $(SRCS)
	@rm -f %(left).new
	cat %(right) | sed -n -e '/^Export/s//extern/p' > %(left).new
	cmp %(left) %(left).new || mv -f %(left).new %(left)
	@rm -f %(left).new

# too annoying
#
#$(OBJS) : $(PROTOS)
#	rm -f %(left)

$(COBJS) : $(CSRCS)
	$(CC) $(CFLAGS) $(EXTRADEFS) %(right) -o %(left) -c

$(EOBJS) : $(ETMPS)
	$(CC) $(CFLAGS) $(EXTRADEFS) %(right) -o %(left) -c

$(ETMPS) : $(ESRCS)
	$(BIN)/drd_cursor %(right) %(left)

$(EXE) : $(PROTOS) $(OBJS)
	$(CC) $(CFLAGS) %(right:"*.o") $(LFLAGS) -o %(left)

# All utilities depend on all libraries
#
#$(UTILS) :: $(LIBARCHIVES)
#	rm -f %(left)

$(CUTILS) : $(CSRCS)
	$(CC) $(CFLAGS) $(EXTRADEFS) %(right) $(LFLAGS) -o %(left)

$(EUTILS) : $(ETMPS)
	$(CC) $(CFLAGS) $(EXTRADEFS) %(right) $(LFLAGS) -o %(left)

$(LIBNAME) : $(PROTOS) $(EXPORTS) $(OBJS)
	@rm -f %(left) %(left).new
	ar cr %(left).new %(right:"*.o")
	ranlib %(left).new
	mv -f %(left).new %(left)

$(IUTILS) : $(UTILS)
	rm -f %(left).new
	cp %(right) %(left).new
	chmod 755 %(left).new
	mv -f %(left).new %(left)

$(IEXE) : $(EXE)
	rm -f %(left).new
	cp %(right) %(left).new
	chmod 755 %(left).new
	mv -f %(left).new %(left)

$(ILIB) : $(LIBNAME)
	rm -f %(left).new
	cp %(right) %(left).new
	chmod 644 %(left).new
	mv -f %(left).new %(left)

