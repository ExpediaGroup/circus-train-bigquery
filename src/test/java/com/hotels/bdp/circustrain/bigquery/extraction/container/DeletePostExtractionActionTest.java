package com.hotels.bdp.circustrain.bigquery.extraction.container;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;

@RunWith(MockitoJUnitRunner.class)
public class DeletePostExtractionActionTest {

  private @Mock Table table;

  @Test
  public void typical() {
    PostExtractionAction deleteAction = new DeletePostExtractionAction(table);
    deleteAction.run();
    verify(table).delete();
  }

}
