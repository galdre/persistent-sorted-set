package me.tonsky.persistent_sorted_set;

import java.util.*;
import clojure.lang.*;

@SuppressWarnings("unchecked")
public class Seq extends ASeq implements IReduce, Reversible, IChunkedSeq, ISeek{
  final PersistentSortedSet _set;
  Seq   _parent;
  ANode _node;
  int   _idx;
  final ANode _finalNodeFirstKey;
  final int _finalIdx;
  final boolean _isFinalLeaf;
  final Object _keyTo;
  final Comparator _cmp;
  final boolean _asc;
  final int _version;

  Seq(IPersistentMap meta, PersistentSortedSet set, Seq parent, ANode node, int idx, Object keyTo, Comparator cmp, boolean asc, int version) {
    super(meta);
    _set = set;
    _parent = parent;
    _node   = node;
    _idx    = idx;
    _keyTo  = keyTo;
    _cmp    = cmp;
    _asc    = asc;
    _version = version;
    _finalNodeFirstKey = null;
    _finalIdx = asc ? Integer.MAX_VALUE : 0;
    _isFinalLeaf = false;
  }

  Seq(IPersistentMap meta, PersistentSortedSet set, Seq parent, ANode node, int idx, ANode final_node, int finalIdx, Object keyTo, Comparator cmp, boolean asc, int version) {
    super(meta);
    _set = set;
    _parent = parent;
    _node   = node;
    _idx    = idx;
    _keyTo  = keyTo;
    _cmp    = cmp;
    _asc    = asc;
    _version = version;
    _finalNodeFirstKey = final_node._keys[0];
    _finalIdx = finalIdx;
    _isFinalLeaf = isFinalLeaf();
  }

  private boolean isFinalLeaf() {
    if (_finalNodeFirstKey == null) return false;
    int d = _cmp.compare(_node._keys[0], _finalNodeFirstKey);
    return d == 0;
  }

  void checkVersion() {
    if (_version != _set._version)
      throw new RuntimeException("Tovarisch, you are iterating and mutating a transient set at the same time!");
  }

  ANode child() {
    assert _node instanceof Branch : _node;
    return ((Branch) _node).child(_set._storage, _idx);
  }

  boolean over() {
    if (_keyTo == null) return false;
    if (_finalNodeFirstKey == null) {
      int d = _cmp.compare(first(), _keyTo);
      return _asc ? d > 0 : d < 0;
    } else {
      if (_asc) {
        // Test > vs >=
        return _isFinalLeaf && _idx > _finalIdx;
      } else {
        return _isFinalLeaf && _idx < _finalIdx;
      }
      
    }
  }

  boolean advance() {
    checkVersion();
    if (_asc) {
      if (_idx < _node._len - 1) {
        _idx++;
        return !over();
      } else if (_parent != null) {
        _parent = _parent.next();
        if (_parent != null) {
          _node = _parent.child();
          _idx = 0;
          _isFinalLeaf = isFinalLeaf();
          return !over();
        }
      }
    } else { // !_asc
      if (_idx > 0) {
        _idx--;
        return !over();
      } else if (_parent != null) {
        _parent = _parent.next();
        if (_parent != null) {
          _node = _parent.child();
          _idx = _node._len - 1;
          _isFinalLeaf = isFinalLeaf();
          return !over();
        }
      }
    }
    return false;
  }

  protected Seq clone() {
    return new Seq(meta(), _set, _parent, _node, _idx, _finalNodeFirstKey, _finalIdx, _keyTo, _cmp, _asc, _version);
  }

  // ASeq
  public Object first() {
    checkVersion();
    // assert _node.leaf();
    return _node._keys[_idx];
  }

  public Seq next() {
    Seq next = clone();
    return next.advance() ? next : null;
  }

  public Obj withMeta(IPersistentMap meta) {
    if (meta() == meta) return this;
    return new Seq(meta, _set, _parent, _node, _idx, _finalNodeFirstKey, _idx, _keyTo, _cmp, _asc, _version);
  }

  // IReduce
  public Object reduce(IFn f) {
    checkVersion();
    Seq clone = clone();
    Object ret = clone.first();
    while (clone.advance()) {
      ret = f.invoke(ret, clone.first());
      if (ret instanceof Reduced)
        return ((Reduced) ret).deref();
    }
    return ret;
  }

  public Object reduce(IFn f, Object start) {
    checkVersion();
    Seq clone = clone();
    Object ret = start;
    do {
      ret = f.invoke(ret, clone.first());
      if (ret instanceof Reduced)
        return ((Reduced) ret).deref();
    } while (clone.advance());
    return ret;
  }

  // Iterable
  public Iterator iterator() { checkVersion(); return new JavaIter(clone()); }

  // IChunkedSeq
  public Chunk chunkedFirst() { checkVersion(); return new Chunk(this); }

  public Seq chunkedNext() {
    checkVersion();
    if (_parent == null) return null;
    Seq nextParent = _parent.next();
    if (nextParent == null) return null;
    ANode node = nextParent.child();
    Seq seq = new Seq(meta(), _set, nextParent, node, _asc ? 0 : node._len - 1, _finalNodeFirstKey, _finalIdx, _keyTo, _cmp, _asc, _version);
    return seq.over() ? null : seq;
  }

  public ISeq chunkedMore() {
    Seq seq = chunkedNext();
    if (seq == null) return PersistentList.EMPTY;
    return seq;
  }

  // Reversible
  boolean atBeginning() {
    return _idx == 0 && (_parent == null || _parent.atBeginning());
  }

  boolean atEnd() {
    return _idx == _node._len-1 && (_parent == null || _parent.atEnd());
  }

  public Seq rseq() {
    checkVersion();
    if (_asc)
      return _set.rslice(_keyTo, atBeginning() ? null : first(), _cmp);
    else
      return _set.slice(_keyTo, atEnd() ? null : first(), _cmp);
  }

  public Seq seek(Object to) { return seek(to, _cmp); }
  public Seq seek(Object to, Comparator cmp) {
    if (to == null) throw new RuntimeException("seek can't be called with a nil key!");
    if (_keyTo != null) {
      int d = _cmp.compare(to, _keyTo);
      // Empty seq if we're seeking past the end??
      if ((_asc && d > 0) || (!_asc && d < 0)) {
        return null;
      }
    }
    Seq seq = this._parent;
    ANode node = this._node;

    if (_asc) {

      while (node != null && cmp.compare(node.maxKey(), to) < 0){
        if (seq == null) {
          return null;
        } else {
          node = seq._node;
          seq = seq._parent;
        }
      }

      while (true) {
        int idx = node.searchFirst(to, cmp);
        if (idx < 0)
          idx = -idx - 1;
        if (idx == node._len)
          return null;
        if (node instanceof Branch) {
          seq = new Seq(null, this._set, seq, node, idx, _finalNodeFirstKey, _finalIdx, null, null, true, _version);
          node = seq.child();
        } else { // Leaf
          seq = new Seq(null, this._set, seq, node, idx, _finalNodeFirstKey, _finalIdx, _keyTo, cmp, true, _version);
          return seq.over() ? null : seq;
        }
      }

    } else {

      // NOTE: We can't shortcircuit here as we don't know the minKey. Might go up one level too high.
      while (cmp.compare(to, node.minKey()) < 0 && seq != null){
        node = seq._node;
        seq = seq._parent;
      }

      while (true) {
        if (node instanceof Branch) {
          int idx = node.searchLast(to, cmp) + 1;
          if (idx == node._len) --idx; // last or beyond, clamp to last
          seq = new Seq(null, this._set, seq, node, idx, _finalNodeFirstKey, _finalIdx, null, null, false, _version);
          node = seq.child();
        } else { // Leaf
          int idx = node.searchLast(to, cmp);
          if (idx == -1) { // not in this, so definitely in prev
            seq = new Seq(null, this._set, seq, node, 0, _finalNodeFirstKey, _finalIdx, this._keyTo, cmp, false, _version);
            return seq.advance() ? seq : null;
          } else { // exact match
            seq = new Seq(null, this._set, seq, node, idx, _finalNodeFirstKey, _finalIdx, this._keyTo, cmp, false, _version);
            return seq.over() ? null : seq;
          }
        }
      }
    }
  }
}
