package io.delta.standalone.actions;

public class SingleAction {

  public SingleAction(AddFile add) {
    this.add = add;
  }

  public SingleAction(Metadata metaData) {
    this.metaData = metaData;
  }

  public SingleAction(Protocol protocol) {
    this.protocol = protocol;
  }

  public SingleAction(CommitInfo commitInfo) {
    this.commitInfo = commitInfo;
  }

  private AddFile add;
  private Metadata metaData;
  private Protocol protocol;
  private CommitInfo commitInfo;

  Action unwrap() {
    if (add != null) {
      return add;
    } else if (metaData != null) {
      return metaData;
    } else if (protocol != null) {
      return protocol;
    } else if (commitInfo != null) {
      return commitInfo;
    } else {
      return null;
    }
  }
}
